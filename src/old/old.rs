use amplify::Wrapper;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::io::{self, Read, Write};
use std::net::{self, TcpStream, ToSocketAddrs};
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Duration;

use cyphernet::addr::{Addr, Host, PeerAddr, ToSocketAddr};
use cyphernet::crypto::ed25519::{PrivateKey, PublicKey, Sign};
use cyphernet::crypto::{EcPk, Ecdh};
use cyphernet::noise::framing::{NoiseDecryptor, NoiseEncryptor, NoiseState, NoiseTranscoder};
use cyphernet::noise::xk::NoiseXkState;
use ed25519_compact::x25519;

use crate::auth::Authenticator;
use crate::connection::Proxy;
use crate::{Address, NetConnection, NetSession, SplitIo, SplitIoError};

pub trait PeerId: EcPk {}
impl<T> PeerId for T where T: EcPk {}

#[derive(Getters, Clone, Eq, PartialEq)]
pub struct NodeKeys<E: Ecdh + Sign> {
    pk: E::Pk,
    ecdh: E,
}

impl<E: Ecdh + Sign> From<E> for NodeKeys<E> {
    fn from(ecdh: E) -> Self {
        NodeKeys {
            pk: ecdh.to_pk(),
            ecdh,
        }
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, From)]
pub enum XkAddr<Id: PeerId, A: Address> {
    #[from]
    Partial(A),

    #[from]
    Full(PeerAddr<Id, A>),
}

impl<Id: PeerId, A: Address> Display for XkAddr<Id, A>
where
    Id: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            XkAddr::Partial(addr) => Display::fmt(addr, f),
            XkAddr::Full(addr) => Display::fmt(addr, f),
        }
    }
}

impl<Id: PeerId, A: Address> Host for XkAddr<Id, A> {}
impl<Id: PeerId, A: Address> Addr for XkAddr<Id, A> {
    fn port(&self) -> u16 {
        match self {
            XkAddr::Partial(a) => a.port(),
            XkAddr::Full(a) => a.port(),
        }
    }
}

impl<Id: PeerId, A: Address + ToSocketAddr> ToSocketAddr for XkAddr<Id, A> {
    fn to_socket_addr(&self) -> net::SocketAddr {
        match self {
            XkAddr::Partial(a) => a.to_socket_addr(),
            XkAddr::Full(a) => a.to_socket_addr(),
        }
    }
}

impl<Id: PeerId, A: Address + ToSocketAddrs> ToSocketAddrs for XkAddr<Id, A> {
    type Iter = A::Iter;

    fn to_socket_addrs(&self) -> io::Result<Self::Iter> {
        match self {
            XkAddr::Partial(a) => a.to_socket_addrs(),
            XkAddr::Full(a) => a.addr().to_socket_addrs(),
        }
    }
}

impl<Id: PeerId, A: Address> From<XkAddr<Id, A>> for net::SocketAddr
where
    A: ToSocketAddr,
{
    fn from(addr: XkAddr<Id, A>) -> Self {
        addr.to_socket_addr()
    }
}

impl<Id: PeerId, A: Address> XkAddr<Id, A> {
    pub fn upgrade(&mut self, id: Id) -> bool {
        match self {
            XkAddr::Partial(addr) => {
                *self = XkAddr::Full(PeerAddr::new(id, addr.clone()));
                true
            }
            XkAddr::Full(_) => false,
        }
    }

    pub fn as_addr(&self) -> &A {
        match self {
            XkAddr::Partial(a) => a,
            XkAddr::Full(a) => a.addr(),
        }
    }

    pub fn peer_id(&self) -> Option<&Id> {
        match self {
            XkAddr::Partial(_) => None,
            XkAddr::Full(addr) => Some(addr.id()),
        }
    }

    pub fn expect_peer_id(&self) -> &Id {
        match self {
            XkAddr::Partial(_) => panic!("handshake is not complete"),
            XkAddr::Full(addr) => addr.id(),
        }
    }

    pub fn expect_peer_addr(&self) -> &PeerAddr<Id, A> {
        match self {
            XkAddr::Partial(_) => panic!("handshake is not complete"),
            XkAddr::Full(addr) => addr,
        }
    }
}

#[derive(Debug)]
pub struct NoiseXkReader<E: Ecdh, S: NetSession = TcpStream> {
    remote_addr: PeerAddr<E::Pk, <S::Connection as NetConnection>::Addr>,
    reader: S::Read,
    decryptor: NoiseDecryptor,
}

#[derive(Debug)]
pub struct NoiseXkWriter<E: Ecdh, S: NetSession = TcpStream> {
    remote_addr: PeerAddr<E::Pk, <S::Connection as NetConnection>::Addr>,
    writer: S::Write,
    encryptor: NoiseEncryptor,
    authenticator: Authenticator,
}

impl<E: Ecdh, S: NetSession> Read for NoiseXkReader<E, S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

impl<E: Ecdh, S: NetSession> Write for NoiseXkWriter<E, S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[derive(Debug)]
pub struct NoiseXk<E: Ecdh, S: NetSession = TcpStream> {
    remote_addr: XkAddr<E::Pk, <S::Connection as NetConnection>::Addr>,
    connection: S,
    transcoder: NoiseTranscoder<NoiseXkState>,
    authenticator: Authenticator,
}

impl<E: Ecdh, S: NetSession> AsRawFd for NoiseXk<E, S> {
    fn as_raw_fd(&self) -> RawFd {
        self.connection.as_raw_fd()
    }
}

impl<E: Ecdh, S: NetSession> Read for NoiseXk<E, S>
where
    E::Pk: From<ed25519_compact::PublicKey>,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.transcoder.is_handshake_complete() {
            let mut input = vec![0u8; self.transcoder.next_handshake_len()];
            self.connection.read_exact(&mut input)?;
            log::trace!(target: "handshake", "Received {input:02x?}");
            let act = self
                .transcoder
                .advance_handshake(&input)
                .map_err(|err| io::Error::new(io::ErrorKind::ConnectionAborted, err))?;
            return match act {
                None => Ok(0),
                Some(act) => {
                    log::trace!(target: "handshake", "Sent {act:02x?}");
                    self.connection.write_all(&act)?;
                    Ok(0)
                }
            };
        } else if !self.authenticator.is_auth_complete() {
            return match (
                self.authenticator.verify(&mut self.connection)?,
                self.remote_addr.peer_id(),
            ) {
                (Some(remote_id), Some(peer_id))
                    if &E::Pk::from(remote_id.into_inner()) != peer_id =>
                {
                    log::error!(target: "authentication",
                        "Remote peer has a different identity {remote_id} than expected",
                    );
                    Err(io::ErrorKind::ConnectionReset.into())
                }
                (None, _) => Err(io::ErrorKind::InvalidInput.into()),
                (Some(remote_id), _) => {
                    self.remote_addr.upgrade(remote_id.into_inner().into());
                    Ok(0)
                }
            };
        }
        self.connection.read(buf)
    }
}

impl<E: Ecdh, S: NetSession> Write for NoiseXk<E, S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.transcoder.is_handshake_complete() {
            let act = self
                .transcoder
                .advance_handshake(&[])
                .map_err(|err| io::Error::new(io::ErrorKind::ConnectionAborted, err))?;
            if let Some(next_act) = act {
                log::trace!(target: "handshake", "Sent {next_act:02x?}");
                self.connection.write_all(&next_act)?
            }
            return Err(io::ErrorKind::Interrupted.into());
        } else if !self.authenticator.is_auth_sent() {
            self.authenticator.certify(&mut self.connection)?;
            return Err(io::ErrorKind::Interrupted.into());
        }
        self.connection.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.connection.flush()
    }
}

impl<S: NetSession<Context = ()>> NetSession for NoiseXk<PrivateKey, S>
where
    S::PeerAddr: From<PeerAddr<PublicKey, <S::Connection as NetConnection>::Addr>>,
{
    type Context = (PrivateKey, Authenticator);
    type Id = PublicKey;
    type PeerAddr = PeerAddr<PublicKey, <S::Connection as NetConnection>::Addr>;
    type TransientAddr = XkAddr<PublicKey, <S::Connection as NetConnection>::Addr>;

    fn accept(connection: Self::Connection, context: Self::Context) -> io::Result<Self> {
        let ecdh =
            x25519::SecretKey::from_ed25519(context.0.as_inner()).expect("invalid local node key");
        Ok(Self {
            authenticator: context.1,
            remote_addr: XkAddr::Partial(connection.remote_addr()),
            connection: S::accept(connection, ())?,
            transcoder: NoiseTranscoder::with_xk_responder(ecdh),
        })
    }

    fn connect_blocking<P: Proxy>(
        peer_addr: Self::PeerAddr,
        context: Self::Context,
        proxy: &P,
    ) -> Result<Self, P::Error> {
        let ecdh =
            x25519::SecretKey::from_ed25519(context.0.as_inner()).expect("invalid local node key");
        let remote_key = x25519::PublicKey::from_ed25519(peer_addr.id().as_inner())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;

        let peer_id = *peer_addr.id();
        let remote_addr = XkAddr::Full(peer_addr.clone());

        let mut connection = S::connect_blocking(peer_addr.into(), (), proxy)?;
        let mut transcoder = NoiseTranscoder::with_xk_initiator(ecdh, remote_key);

        // Handshake
        let mut input = vec![];
        while !transcoder.is_handshake_complete() {
            let act = transcoder
                .advance_handshake(&input)
                .map_err(|err| io::Error::new(io::ErrorKind::ConnectionAborted, err))?;
            if let Some(act) = act {
                log::trace!(target: "handshake", "Sent {act:02x?}");
                connection.write_all(&act)?;
            }
            if !transcoder.is_handshake_complete() {
                input = vec![0u8; transcoder.next_handshake_len()];
                connection.read_exact(&mut input)?;
                log::trace!(target: "handshake", "Received {input:02x?}");
            }
        }

        let mut authenticator = context.1;
        while !authenticator.is_auth_complete() {
            authenticator.certify(&mut connection)?;
            match authenticator.verify(&mut connection)? {
                None => {
                    log::error!(target: "authentication", "The remote peer has failed validation");
                    // TODO: Return proper error type
                    panic!()
                }
                Some(id) if id != peer_id => {
                    log::error!(target: "authentication", "The remote peer has a different identity than expected");
                    // TODO: Return proper error type
                    panic!()
                }
                _ => {}
            }
        }

        Ok(Self {
            authenticator,
            remote_addr,
            connection,
            transcoder,
        })
    }

    #[cfg(feature = "connect_nonblocking")]
    fn connect_nonblocking<P: Proxy>(
        peer_addr: Self::PeerAddr,
        context: Self::Context,
        proxy: &P,
    ) -> Result<Self, P::Error> {
        let ecdh =
            x25519::SecretKey::from_ed25519(context.0.as_inner()).expect("invalid local node key");
        let remote_key = x25519::PublicKey::from_ed25519(peer_addr.id().as_inner())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let remote_addr = XkAddr::Full(peer_addr.clone());

        let socket = S::connect_nonblocking(peer_addr.into(), (), proxy)?;

        Ok(Self {
            authenticator: context.1,
            remote_addr,
            connection: socket,
            transcoder: NoiseTranscoder::with_xk_initiator(ecdh, remote_key),
        })
    }

    fn session_id(&self) -> Option<Self::Id> {
        match &self.remote_addr {
            XkAddr::Partial(_) => None,
            XkAddr::Full(a) => Some(*a.id()),
        }
    }

    fn is_established(&self) -> bool {
        self.transcoder.is_handshake_complete() && self.authenticator.is_auth_complete()
    }

    fn transient_addr(&self) -> Self::TransientAddr {
        self.remote_addr.clone()
    }

    fn peer_addr(&self) -> Option<Self::PeerAddr> {
        match self.remote_addr {
            XkAddr::Partial(_) => None,
            XkAddr::Full(ref addr) => Some(addr.clone()),
        }
    }

    fn local_addr(&self) -> <S::Connection as NetConnection>::Addr {
        self.connection.local_addr()
    }

    fn read_timeout(&self) -> io::Result<Option<Duration>> {
        self.connection.read_timeout()
    }

    fn write_timeout(&self) -> io::Result<Option<Duration>> {
        self.connection.write_timeout()
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        self.connection.set_read_timeout(dur)
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        self.connection.set_write_timeout(dur)
    }

    fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()> {
        self.connection.set_nonblocking(nonblocking)
    }

    fn disconnect(self) -> io::Result<()> {
        self.connection.disconnect()
    }
}
