use std::fmt::Debug;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Duration;
use std::{io, net};

use reactor::poller::IoType;
use reactor::{Io, IoStatus, ReadNonblocking, Resource, WriteNonblocking};

use crate::{NetConnection, NetListener, NetSession};

/// Socket read buffer size.
const READ_BUFFER_SIZE: usize = u16::MAX as usize;
/// Maximum time to wait when reading from a socket.
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(6);
/// Maximum time to wait when writing to a socket.
const WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Debug)]
pub enum ListenerEvent<S: NetSession> {
    Accepted(S),
    Failure(io::Error),
}

#[derive(Debug)]
pub struct NetAccept<S: NetSession, L: NetListener<Stream = S::Connection> = TcpListener> {
    session_context: S::Context,
    listener: L,
}

impl<L: NetListener<Stream = S::Connection>, S: NetSession> AsRawFd for NetAccept<S, L> {
    fn as_raw_fd(&self) -> RawFd {
        self.listener.as_raw_fd()
    }
}

impl<L: NetListener<Stream = S::Connection>, S: NetSession> io::Write for NetAccept<S, L> {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        panic!("must not write to network listener")
    }

    fn flush(&mut self) -> io::Result<()> {
        panic!("must not write to network listener")
    }
}

impl<L: NetListener<Stream = S::Connection>, S: NetSession> WriteNonblocking for NetAccept<S, L> {
    fn set_write_nonblocking(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        panic!("must not write to network listener")
    }

    fn can_write(&self) -> bool {
        true
    }

    fn write_nonblocking(&mut self, buf: &[u8]) -> IoStatus {
        panic!("must not write to network listener")
    }

    fn flush_nonblocking(&mut self) -> IoStatus {
        panic!("must not write to network listener")
    }
}

impl<L: NetListener<Stream = S::Connection>, S: NetSession> NetAccept<S, L> {
    pub fn bind(addr: impl Into<net::SocketAddr>, session_context: S::Context) -> io::Result<Self> {
        let listener = L::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            session_context,
            listener,
        })
    }

    pub fn local_addr(&self) -> net::SocketAddr {
        self.listener.local_addr()
    }

    fn handle_accept(&mut self) -> io::Result<S> {
        let mut stream = self.listener.accept()?;
        stream.set_read_timeout(Some(READ_TIMEOUT))?;
        stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
        stream.set_nonblocking(true)?;
        S::accept(stream, &self.session_context)
    }
}

impl<L: NetListener<Stream = S::Connection>, S: NetSession> Resource for NetAccept<S, L> {
    type Id = net::SocketAddr;
    type Event = ListenerEvent<S>;

    fn id(&self) -> Self::Id {
        self.listener.local_addr()
    }

    fn interests(&self) -> IoType {
        IoType::read_only()
    }

    fn handle_io(&mut self, io: Io) -> Option<Self::Event> {
        match io {
            Io::Read => Some(match self.handle_accept() {
                Err(err) => ListenerEvent::Failure(err),
                Ok(session) => ListenerEvent::Accepted(session),
            }),
            Io::Write => None,
        }
    }

    fn disconnect(self) -> io::Result<()> {
        // We disconnect by dropping the self
        Ok(())
    }
}

pub enum SessionEvent<S: NetSession> {
    Established(S::Id),
    Data(Vec<u8>),
    Terminated(io::Error),
}

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub enum TransportState {
    Init,
    Handshake,
    Active,
    Terminated,
}

pub struct NetTransport<S: NetSession> {
    state: TransportState,
    session: S,
    inbound: bool,
    needs_flush: bool,
    buffer: Vec<u8>,
    buffer_len: usize,
}

impl<S: NetSession> AsRawFd for NetTransport<S> {
    fn as_raw_fd(&self) -> RawFd {
        self.session.as_raw_fd()
    }
}

impl<S: NetSession> NetSession for NetTransport<S> {
    type Context = S::Context;
    type Connection = S::Connection;
    type Id = S::Id;
    type PeerAddr = S::PeerAddr;
    type TransientAddr = S::TransientAddr;

    fn accept(connection: Self::Connection, context: &Self::Context) -> io::Result<Self> {
        S::accept(connection, context).and_then(NetTransport::accept)
    }

    fn connect(
        addr: Self::PeerAddr,
        context: &Self::Context,
        nonblocking: bool,
    ) -> io::Result<Self> {
        NetTransport::connect(addr, context, nonblocking)
    }

    fn id(&self) -> Option<Self::Id> {
        self.session.id()
    }

    fn handshake_completed(&self) -> bool {
        self.session.handshake_completed()
    }

    fn transient_addr(&self) -> Self::TransientAddr {
        self.session.transient_addr()
    }

    fn peer_addr(&self) -> Option<Self::PeerAddr> {
        self.session.peer_addr()
    }

    fn local_addr(&self) -> <Self::Connection as NetConnection>::Addr {
        self.session.local_addr()
    }

    fn read_timeout(&self) -> io::Result<Option<Duration>> {
        self.session.read_timeout()
    }

    fn write_timeout(&self) -> io::Result<Option<Duration>> {
        self.session.write_timeout()
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        self.session.set_read_timeout(dur)
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        self.session.set_write_timeout(dur)
    }

    fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()> {
        self.session.set_nonblocking(nonblocking)
    }

    fn disconnect(self) -> io::Result<()> {
        self.session.disconnect()
    }
}

impl<S: NetSession> NetTransport<S> {
    fn with(mut session: S, inbound: bool, state: TransportState) -> io::Result<Self> {
        session.set_read_timeout(Some(READ_TIMEOUT))?;
        session.set_write_timeout(Some(WRITE_TIMEOUT))?;
        Ok(Self {
            state,
            session,
            inbound,
            needs_flush: false,
            buffer: vec![0; READ_BUFFER_SIZE],
            buffer_len: 0,
        })
    }

    pub fn accept(session: S) -> io::Result<Self> {
        Self::with(session, true, TransportState::Handshake)
    }

    pub fn connect(addr: S::PeerAddr, context: &S::Context, nonblocking: bool) -> io::Result<Self> {
        let mut session = S::connect(addr, context, nonblocking)?;
        session.set_nonblocking(nonblocking)?;
        let state = if nonblocking {
            TransportState::Init
        } else {
            TransportState::Handshake
        };
        Self::with(session, false, state)
    }

    pub fn is_inbound(&self) -> bool {
        self.inbound
    }

    pub fn is_outbound(&self) -> bool {
        !self.is_inbound()
    }

    pub fn state(&self) -> TransportState {
        self.state
    }

    pub fn local_addr(&self) -> <S::Connection as NetConnection>::Addr {
        self.session.local_addr()
    }

    pub fn remote_addr(&self) -> Option<S::PeerAddr> {
        self.session.peer_addr()
    }

    pub fn transient_addr(&self) -> S::TransientAddr {
        self.session.transient_addr()
    }

    pub fn peer_id(&self) -> Option<S::Id> {
        self.session.id()
    }

    pub fn expect_peer_id(&self) -> S::Id {
        self.session.expect_id()
    }

    pub fn drain_buffer(&mut self) -> Vec<u8> {
        let len = self.buffer_len;
        self.buffer_len = 0;
        self.buffer[..len].to_vec()
    }

    fn terminate(&mut self, reason: io::Error) -> SessionEvent<S> {
        #[cfg(feature = "log")]
        log::debug!(target: "transport", "Terminating connection {} due to {reason:?}", self.session.as_raw_fd());

        self.state = TransportState::Terminated;
        SessionEvent::Terminated(reason)
    }

    fn handle_writable(&mut self) -> Option<SessionEvent<S>> {
        self.needs_flush = false;
        match self.session.flush_nonblocking() {
            IoStatus::Success(_) | IoStatus::WouldBlock => None,
            IoStatus::Shutdown => Some(self.terminate(io::ErrorKind::WriteZero.into())),
            IoStatus::Err(err) => Some(self.terminate(err)),
        }
    }

    fn handle_readable(&mut self) -> Option<SessionEvent<S>> {
        // Nb. Since `poll`, which this reactor is based on, is *level-triggered*,
        // we will be notified again if there is still data to be read on the socket.
        // Hence, there is no use in putting this socket read in a loop, as the second
        // invocation would likely block.
        match self
            .session
            .read_nonblocking(&mut self.buffer[self.buffer_len..])
        {
            IoStatus::Success(0) => None,
            IoStatus::Success(len) => {
                self.buffer_len += len;
                Some(SessionEvent::Data(self.drain_buffer()))
            }
            IoStatus::Shutdown => Some(self.terminate(io::ErrorKind::Interrupted.into())),
            IoStatus::WouldBlock => Some(SessionEvent::Data(self.drain_buffer())),
            IoStatus::Err(err) => Some(self.terminate(err)),
        }
    }
}

impl<S: NetSession> Resource for NetTransport<S>
where
    S::TransientAddr: Into<net::SocketAddr>,
{
    type Id = RawFd;
    type Event = SessionEvent<S>;

    fn id(&self) -> Self::Id {
        self.session.as_raw_fd()
    }

    fn interests(&self) -> IoType {
        match self.state {
            TransportState::Init => IoType::read_write(),
            TransportState::Terminated => IoType::none(),
            TransportState::Active | TransportState::Handshake if self.needs_flush => {
                IoType::read_write()
            }
            TransportState::Active | TransportState::Handshake => IoType::read_only(),
        }
    }

    fn handle_io(&mut self, io: Io) -> Option<Self::Event> {
        debug_assert_ne!(
            self.state,
            TransportState::Terminated,
            "I/O on terminated transport"
        );

        if self.state == TransportState::Init {
            #[cfg(feature = "log")]
            log::trace!(target: "transport", "Transport {} is connected, initializing handshake", Resource::id(self));

            self.state = TransportState::Handshake;
        } else if self.state == TransportState::Handshake {
            debug_assert_eq!(self.buffer_len, 0);
            debug_assert!(!self.session.handshake_completed());
            #[cfg(feature = "log")]
            log::trace!(target: "transport", "Transport {} got I/O while in handshake mode", Resource::id(self));
        }

        let resp = match io {
            Io::Read => self.handle_readable(),
            Io::Write => self.handle_writable(),
        };

        if self.session.handshake_completed() && self.state == TransportState::Handshake {
            // During the handshake only termination can happen, but in this case
            // we would not be in a `TransportState::Handshake` state anymore
            debug_assert!(resp.is_none());

            #[cfg(feature = "log")]
            log::trace!(target: "transport", "Handshake for {} is complete", Resource::id(self));

            self.state = TransportState::Active;
            Some(SessionEvent::Established(self.session.expect_id()))
        } else {
            resp
        }
    }

    fn disconnect(self) -> io::Result<()> {
        self.session.disconnect()
    }
}

impl<S: NetSession> Read for NetTransport<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.state == TransportState::Init {
            return Err(io::ErrorKind::NotConnected.into());
        }
        self.session.read(buf)
    }
}
impl<S: NetSession> ReadNonblocking for NetTransport<S> {
    fn set_read_nonblocking(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.session.set_read_timeout(timeout)
    }
}

impl<S: NetSession> Write for NetTransport<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.state == TransportState::Init {
            return Err(io::ErrorKind::NotConnected.into());
        }
        self.session.write(&buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.state == TransportState::Init {
            return Err(io::ErrorKind::NotConnected.into());
        }
        self.session.flush()
    }
}
impl<S: NetSession> WriteNonblocking for NetTransport<S> {
    fn set_write_nonblocking(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.session.set_write_nonblocking(timeout)
    }

    fn can_write(&self) -> bool {
        self.state != TransportState::Init
    }

    fn flush_nonblocking(&mut self) -> IoStatus {
        self.needs_flush = false;
        self.session.flush_nonblocking()
    }
}

mod split {
    use super::*;

    #[derive(Debug, Display)]
    #[display("{error}")]
    pub struct SplitIoError<T: SplitIo> {
        pub original: T,
        pub error: io::Error,
    }

    impl<T: SplitIo + Debug> std::error::Error for SplitIoError<T> {}

    pub trait SplitIo: Sized {
        type Read: Read + Sized;
        type Write: Write + Sized;

        /// # Panics
        ///
        /// If the split operation is not possible
        fn split_io(self) -> Result<(Self::Read, Self::Write), SplitIoError<Self>>;
        fn from_split_io(read: Self::Read, write: Self::Write) -> Self;
    }

    pub struct NetReader<S: NetSession> {
        state: TransportState,
        session: <S as SplitIo>::Read,
        inbound: bool,
    }

    impl<S: NetSession> Read for NetReader<S> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.session.read(buf)
        }
    }

    pub struct NetWriter<S: NetSession> {
        state: TransportState,
        session: <S as SplitIo>::Write,
        inbound: bool,
        needs_flush: bool,
    }

    impl<S: NetSession> Write for NetWriter<S> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.needs_flush = true;
            self.session.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.needs_flush = false;
            self.session.flush()
        }
    }

    impl<S: NetSession> SplitIo for NetTransport<S> {
        type Read = NetReader<S>;
        type Write = NetWriter<S>;

        fn split_io(mut self) -> Result<(Self::Read, Self::Write), SplitIoError<Self>> {
            debug_assert_eq!(self.buffer_len, 0);

            match self.session.flush_nonblocking() {
                IoStatus::Success(_) => {}
                IoStatus::WouldBlock => {
                    return Err(SplitIoError {
                        original: self,
                        error: io::ErrorKind::WouldBlock.into(),
                    });
                }
                IoStatus::Shutdown => {
                    return Err(SplitIoError {
                        original: self,
                        error: io::ErrorKind::Interrupted.into(),
                    });
                }
                IoStatus::Err(err) => {
                    return Err(SplitIoError {
                        original: self,
                        error: err,
                    });
                }
            }
            let (r, w) = match self.session.split_io() {
                Err(err) => {
                    self.session = err.original;
                    return Err(SplitIoError {
                        original: self,
                        error: err.error,
                    });
                }
                Ok(s) => s,
            };
            let reader = NetReader {
                state: self.state,
                session: r,
                inbound: self.inbound,
            };
            let writer = NetWriter {
                state: self.state,
                session: w,
                inbound: self.inbound,
                needs_flush: false,
            };
            Ok((reader, writer))
        }

        fn from_split_io(read: Self::Read, write: Self::Write) -> Self {
            debug_assert_eq!(read.state, write.state);
            debug_assert_eq!(read.inbound, write.inbound);
            Self {
                state: read.state,
                inbound: read.inbound,
                session: S::from_split_io(read.session, write.session),
                needs_flush: write.needs_flush,
                buffer: vec![0u8; READ_BUFFER_SIZE],
                buffer_len: 0,
            }
        }
    }
}
pub use split::*;
