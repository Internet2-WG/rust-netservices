use cyphernet::auth::eidolon::EidolonState;
use cyphernet::encrypt::noise::{HandshakePattern, Keyset, NoiseState};
use cyphernet::proxy::socks5;
use std::fmt::{Debug, Display};
use std::io;
use std::net::TcpStream;

use cyphernet::addr::{HostName, InetHost, NetAddr};
use cyphernet::{x25519, Cert, Digest, EcSign};

use crate::{LinkDirection, NetConnection, NetReader, NetStream, NetWriter, SplitIo, SplitIoError};

pub type EidolonSession<I, S> = NetProtocol<EidolonRuntime<I>, S>;
pub type NoiseSession<E, D, S> = NetProtocol<NoiseState<E, D>, S>;
pub type Socks5Session<S> = NetProtocol<socks5::Socks5, S>;

pub type CypherSession<I, D> =
    EidolonSession<I, NoiseSession<x25519::PrivateKey, D, Socks5Session<TcpStream>>>;

pub type EidolonReader<S> = NetReader<S>;
pub type EidolonWriter<I, S> = NetWriter<EidolonRuntime<I>, S>;
pub type CypherReader<D> =
    EidolonReader<NoiseSession<x25519::PrivateKey, D, Socks5Session<TcpStream>>>;
pub type CypherWriter<I, D> =
    EidolonWriter<I, NoiseSession<x25519::PrivateKey, D, Socks5Session<TcpStream>>>;

impl<I: EcSign, D: Digest> CypherSession<I, D> {
    pub fn connect_nonblocking<const HASHLEN: usize>(
        remote_addr: NetAddr<HostName>,
        cert: Cert<I::Sig>,
        allowed_ids: Vec<I::Pk>,
        signer: I,
        proxy_addr: NetAddr<InetHost>,
        force_proxy: bool,
    ) -> io::Result<Self> {
        let connection = if force_proxy {
            TcpStream::connect_nonblocking(proxy_addr)?
        } else {
            TcpStream::connect_nonblocking(remote_addr.connection_addr(proxy_addr))?
        };
        Ok(Self::with_config::<HASHLEN>(
            remote_addr,
            connection,
            LinkDirection::Outbound,
            cert,
            allowed_ids,
            signer,
            force_proxy,
        ))
    }

    pub fn connect_blocking<const HASHLEN: usize>(
        remote_addr: NetAddr<HostName>,
        cert: Cert<I::Sig>,
        allowed_ids: Vec<I::Pk>,
        signer: I,
        proxy_addr: NetAddr<InetHost>,
        force_proxy: bool,
    ) -> io::Result<Self> {
        let connection = if force_proxy {
            TcpStream::connect_blocking(proxy_addr)?
        } else {
            TcpStream::connect_blocking(remote_addr.connection_addr(proxy_addr))?
        };
        let mut session = Self::with_config::<HASHLEN>(
            remote_addr,
            connection,
            LinkDirection::Outbound,
            cert,
            allowed_ids,
            signer,
            force_proxy,
        );
        session.run_handshake()?;
        Ok(session)
    }

    pub fn accept<const HASHLEN: usize>(
        connection: TcpStream,
        cert: Cert<I::Sig>,
        allowed_ids: Vec<I::Pk>,
        signer: I,
    ) -> Self {
        Self::with_config::<HASHLEN>(
            connection.remote_addr().into(),
            connection,
            LinkDirection::Inbound,
            cert,
            allowed_ids,
            signer,
            false,
        )
    }

    fn with_config<const HASHLEN: usize>(
        remote_addr: NetAddr<HostName>,
        connection: TcpStream,
        direction: LinkDirection,
        cert: Cert<I::Sig>,
        allowed_ids: Vec<I::Pk>,
        signer: I,
        force_proxy: bool,
    ) -> Self {
        let socks5 = socks5::Socks5::with(remote_addr, force_proxy);
        let proxy = Socks5Session::with(connection, socks5);

        let noise = NoiseState::initialize::<HASHLEN>(
            HandshakePattern::nn(),
            direction.is_outbound(),
            &[],
            Keyset::noise_nn(),
        );

        let encoding = NoiseSession::with(proxy, noise);
        let eidolon = match direction {
            LinkDirection::Inbound => EidolonRuntime::responder(signer, cert, allowed_ids),
            LinkDirection::Outbound => EidolonRuntime::initiator(signer, cert, allowed_ids),
        };
        let auth = EidolonSession::with(encoding, eidolon);

        auth
    }
}

pub trait NetSession: NetStream + SplitIo {
    /// Inner session type
    type Inner: NetSession;
    /// Underlying connection
    type Connection: NetConnection;
    type Artifact: Display;

    fn is_established(&self) -> bool {
        self.artifact().is_some()
    }
    fn run_handshake(&mut self) -> io::Result<()>;

    fn display(&self) -> String {
        match self.artifact() {
            Some(artifact) => artifact.to_string(),
            None => s!("<no-id>"),
        }
    }
    fn artifact(&self) -> Option<Self::Artifact>;
    fn as_connection(&self) -> &Self::Connection;
    fn as_connection_mut(&mut self) -> &mut Self::Connection;
    fn disconnect(self) -> io::Result<()>;
}

pub trait NetStateMachine {
    const NAME: &'static str;

    type Init: Debug;
    type Artifact;
    type Error: std::error::Error;

    fn init(&mut self, init: Self::Init);
    fn next_read_len(&self) -> usize;
    fn advance(&mut self, input: &[u8]) -> Result<Vec<u8>, Self::Error>;
    fn artifact(&self) -> Option<Self::Artifact>;

    // Blocking
    fn run_handshake(&mut self, stream: &mut impl NetStream) -> io::Result<()> {
        let mut input = vec![];
        while !self.is_complete() {
            let act = self.advance(&input).map_err(|err| {
                #[cfg(feature = "log")]
                log::error!(target: Self::NAME, "Handshake failure: {err}");

                io::Error::from(io::ErrorKind::ConnectionAborted)
            })?;
            if !act.is_empty() {
                #[cfg(feature = "log")]
                log::trace!(target: Self::NAME, "Sending handshake act {act:02x?}");

                stream.write_all(&act)?;
            }
            if !self.is_complete() {
                input = vec![0u8; self.next_read_len()];
                stream.read_exact(&mut input)?;

                #[cfg(feature = "log")]
                log::trace!(target: Self::NAME, "Receiving handshake act {input:02x?}");
            }
        }
        #[cfg(feature = "log")]
        log::debug!(
            target: Self::NAME,
            "Handshake protocol {} successfully completed",
            Self::NAME
        );
        Ok(())
    }

    fn is_init(&self) -> bool;
    fn is_complete(&self) -> bool {
        self.artifact().is_some()
    }
}

pub trait IntoInit<I: Sized> {
    fn into_init(self) -> I;
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct ZeroInit;

impl<T> IntoInit<ZeroInit> for T {
    fn into_init(self) -> ZeroInit {
        ZeroInit
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug, Display)]
#[display("{session}")]
pub struct ProtocolArtifact<M: NetStateMachine, S: NetSession> {
    pub session: S::Artifact,
    pub state: M::Artifact,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct NetProtocol<M: NetStateMachine, S: NetSession>
where
    S::Artifact: IntoInit<M::Init>,
{
    state: M,
    session: S,
}

impl<M: NetStateMachine, S: NetSession> NetProtocol<M, S>
where
    S::Artifact: IntoInit<M::Init>,
{
    pub fn new(session: S) -> Self
    where
        M: Default,
    {
        Self::with(session, M::default())
    }

    pub fn with(session: S, state_machine: M) -> Self {
        Self {
            state: state_machine,
            session,
        }
    }

    fn init(&mut self) -> bool {
        if !self.state.is_init() {
            if let Some(artifact) = self.session.artifact() {
                let init_data = artifact.into_init();

                #[cfg(feature = "log")]
                log::debug!(
                    target: M::NAME,
                    "Initializing state with data {init_data:02x?}"
                );

                self.state.init(init_data);

                return true;
            }
        }
        false
    }
}

impl<M: NetStateMachine, S: NetSession> io::Read for NetProtocol<M, S>
where
    S::Artifact: IntoInit<M::Init>,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.state.is_complete() || !self.session.is_established() {
            return self.session.read(buf);
        }

        self.init();

        let len = self.state.next_read_len();
        let mut input = vec![0u8; len];
        self.session.read_exact(&mut input)?;

        #[cfg(feature = "log")]
        log::trace!(target: M::NAME, "Received handshake act: {input:02x?}");

        if !input.is_empty() {
            let output = self.state.advance(&input).map_err(|err| {
                #[cfg(feature = "log")]
                log::error!(target: M::NAME, "Handshake failure: {err}");

                io::Error::from(io::ErrorKind::ConnectionAborted)
            })?;

            #[cfg(feature = "log")]
            log::trace!(target: M::NAME, "Sending handshake act: {output:02x?}");

            if !output.is_empty() {
                self.session.write_all(&output)?;
            }
        }

        Ok(0)
    }
}

impl<M: NetStateMachine, S: NetSession> io::Write for NetProtocol<M, S>
where
    S::Artifact: IntoInit<M::Init>,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.state.is_complete() || !self.session.is_established() {
            return self.session.write(buf);
        }

        self.init();

        let act = self.state.advance(&[]).map_err(|err| {
            #[cfg(feature = "log")]
            log::error!(target: M::NAME, "Handshake failure: {err}");

            io::Error::from(io::ErrorKind::ConnectionAborted)
        })?;

        if !act.is_empty() {
            #[cfg(feature = "log")]
            log::trace!(target: M::NAME, "Sending handshake act: {act:02x?}");

            self.session.write_all(&act)?;

            Err(io::ErrorKind::Interrupted.into())
        } else {
            self.session.write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.session.flush()
    }
}

impl<M: NetStateMachine, S: NetSession> NetStream for NetProtocol<M, S> where
    S::Artifact: IntoInit<M::Init>
{
}

impl<M: NetStateMachine, S: NetSession> SplitIo for NetProtocol<M, S>
where
    S::Artifact: IntoInit<M::Init>,
{
    type Read = NetReader<S>;
    type Write = NetWriter<M, S>;

    fn split_io(mut self) -> Result<(Self::Read, Self::Write), SplitIoError<Self>> {
        let unique_id = rand::random();

        match self.session.split_io() {
            Err(err) => {
                self.session = err.original;
                Err(SplitIoError {
                    original: self,
                    error: err.error,
                })
            }
            Ok((reader, writer)) => Ok((
                NetReader { unique_id, reader },
                NetWriter {
                    unique_id,
                    state: self.state,
                    writer,
                },
            )),
        }
    }

    fn from_split_io(read: Self::Read, write: Self::Write) -> Self {
        if read.unique_id != write.unique_id {
            panic!("joining into NetProtocol parts not produced by the same split_io()")
        }

        Self {
            state: write.state,
            session: S::from_split_io(read.reader, write.writer),
        }
    }
}

impl<M: NetStateMachine, S: NetSession> NetSession for NetProtocol<M, S>
where
    S::Artifact: IntoInit<M::Init>,
{
    type Inner = S;
    type Connection = S::Connection;
    type Artifact = ProtocolArtifact<M, S>;

    fn run_handshake(&mut self) -> io::Result<()> {
        #[cfg(feature = "log")]
        log::debug!(target: M::NAME, "Starting handshake protocol {}", M::NAME);

        if !self.session.is_established() {
            self.session.run_handshake()?;
        }
        self.init();
        self.state.run_handshake(self.session.as_connection_mut())
    }

    fn artifact(&self) -> Option<Self::Artifact> {
        Some(ProtocolArtifact {
            session: self.session.artifact()?,
            state: self.state.artifact()?,
        })
    }

    fn as_connection(&self) -> &Self::Connection {
        self.session.as_connection()
    }

    fn as_connection_mut(&mut self) -> &mut Self::Connection {
        self.session.as_connection_mut()
    }

    fn disconnect(self) -> io::Result<()> {
        self.session.disconnect()
    }
}

mod imp_std {
    use std::net::{Shutdown, SocketAddr, TcpStream};

    use super::*;

    impl NetSession for TcpStream {
        type Inner = Self;
        type Connection = Self;
        type Artifact = SocketAddr;

        fn run_handshake(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn artifact(&self) -> Option<Self::Artifact> {
            self.peer_addr().ok()
        }

        fn as_connection(&self) -> &Self::Connection {
            self
        }

        fn as_connection_mut(&mut self) -> &mut Self::Connection {
            self
        }

        fn disconnect(self) -> io::Result<()> {
            self.shutdown(Shutdown::Both)
        }
    }
}

#[cfg(feature = "socket2")]
mod imp_socket2 {
    use std::net::{Shutdown, SocketAddr};

    use socket2::Socket;

    use super::*;

    impl NetSession for Socket {
        type Inner = Self;
        type Connection = Self;
        type Artifact = SocketAddr;

        fn run_handshake(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn artifact(&self) -> Option<Self::Artifact> {
            self.peer_addr().ok()?.as_socket()
        }

        fn as_connection(&self) -> &Self::Connection {
            self
        }

        fn as_connection_mut(&mut self) -> &mut Self::Connection {
            self
        }

        fn disconnect(self) -> io::Result<()> {
            self.shutdown(Shutdown::Both)
        }
    }
}

mod imp_eidolon {
    use std::fmt::{self, Display, Formatter};

    use cyphernet::auth::eidolon;
    use cyphernet::display::{Encoding, MultiDisplay};
    use cyphernet::{Cert, CertFormat, Digest, EcSign, Ecdh};

    use super::*;

    pub struct EidolonRuntime<S: EcSign> {
        state: EidolonState<S::Sig>,
        signer: S,
    }

    impl<S: EcSign> EidolonRuntime<S> {
        pub fn initiator(signer: S, cert: Cert<S::Sig>, allowed_ids: Vec<S::Pk>) -> Self {
            Self {
                state: EidolonState::initiator(cert, allowed_ids),
                signer,
            }
        }

        pub fn responder(signer: S, cert: Cert<S::Sig>, allowed_ids: Vec<S::Pk>) -> Self {
            Self {
                state: EidolonState::responder(cert, allowed_ids),
                signer,
            }
        }
    }

    impl<S: EcSign> Display for EidolonRuntime<S> {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            match self.state.remote_cert() {
                Some(cert) => {
                    f.write_str(&cert.display_fmt(&CertFormat::new(", ", Encoding::Base58)))
                }
                None => f.write_str("<unidentified>"),
            }
        }
    }

    impl<S: EcSign> NetStateMachine for EidolonRuntime<S> {
        const NAME: &'static str = "eidolon";
        type Init = Vec<u8>;
        type Artifact = Cert<S::Sig>;
        type Error = eidolon::Error<S::Pk>;

        fn init(&mut self, init: Self::Init) {
            self.state.init(init)
        }

        fn next_read_len(&self) -> usize {
            self.state.next_read_len()
        }

        fn advance(&mut self, input: &[u8]) -> Result<Vec<u8>, Self::Error> {
            self.state.advance(input, &self.signer)
        }

        fn artifact(&self) -> Option<Self::Artifact> {
            self.state.remote_cert().cloned()
        }

        fn is_init(&self) -> bool {
            self.state.is_init()
        }
    }

    impl<S: NetSession, E: Ecdh, D: Digest> IntoInit<Vec<u8>>
        for ProtocolArtifact<NoiseState<E, D>, S>
    {
        fn into_init(self) -> Vec<u8> {
            self.state.as_ref().to_vec()
        }
    }
}
pub use imp_eidolon::EidolonRuntime;

mod impl_noise {
    use cyphernet::encrypt::noise::error::NoiseError;
    use cyphernet::encrypt::noise::NoiseState;
    use cyphernet::{Digest, Ecdh};

    use super::*;

    impl<E: Ecdh, D: Digest> NetStateMachine for NoiseState<E, D> {
        const NAME: &'static str = "noise";
        type Init = ZeroInit;
        type Artifact = D::Output;
        type Error = NoiseError;

        fn init(&mut self, _: Self::Init) {}

        fn next_read_len(&self) -> usize {
            self.next_read_len()
        }

        fn advance(&mut self, input: &[u8]) -> Result<Vec<u8>, Self::Error> {
            self.advance(input)
        }

        fn artifact(&self) -> Option<Self::Artifact> {
            self.get_handshake_hash()
        }

        fn is_init(&self) -> bool {
            true
        }
    }
}

mod impl_socks5 {
    use cyphernet::proxy::socks5;
    use cyphernet::proxy::socks5::Socks5;

    use super::*;

    impl NetStateMachine for Socks5 {
        const NAME: &'static str = "socks5";
        type Init = ZeroInit;
        type Artifact = ();
        type Error = socks5::Error;

        fn init(&mut self, _: Self::Init) {}

        fn next_read_len(&self) -> usize {
            self.next_read_len()
        }

        fn advance(&mut self, input: &[u8]) -> Result<Vec<u8>, Self::Error> {
            self.advance(input)
        }

        fn artifact(&self) -> Option<Self::Artifact> {
            Some(())
        }

        fn is_init(&self) -> bool {
            true
        }
    }
}
