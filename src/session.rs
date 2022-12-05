use std::io;
use std::net;
use std::os::unix::io::AsRawFd;
use std::time::Duration;

use cyphernet::addr::Addr;

use crate::{IoStream, NetConnection};

pub trait NetSession: IoStream + AsRawFd + Sized {
    type Context;
    type Connection: NetConnection;
    type RemoteAddr: Addr + Clone;
    type PeerAddr: Addr;

    fn accept(connection: Self::Connection, context: &Self::Context) -> Self;
    fn connect(addr: Self::RemoteAddr, context: &Self::Context) -> io::Result<Self>;

    fn peer_addr(&self) -> Self::PeerAddr;

    fn read_timeout(&self) -> io::Result<Option<Duration>>;
    fn write_timeout(&self) -> io::Result<Option<Duration>>;
    fn set_read_timeout(&mut self, dur: Option<Duration>) -> io::Result<()>;
    fn set_write_timeout(&mut self, dur: Option<Duration>) -> io::Result<()>;

    fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()>;
}

#[cfg(feature = "socket2")]
impl NetSession for net::TcpStream {
    type Context = ();
    type Connection = Self;
    type RemoteAddr = net::SocketAddr;
    type PeerAddr = net::SocketAddr;

    fn accept(connection: Self::Connection, _context: &Self::Context) -> Self {
        connection
    }

    fn connect(addr: Self::RemoteAddr, _context: &Self::Context) -> io::Result<Self> {
        Self::connect_nonblocking(addr)
    }

    fn peer_addr(&self) -> Self::PeerAddr {
        <Self as NetConnection>::peer_addr(self)
    }

    fn read_timeout(&self) -> io::Result<Option<Duration>> {
        <Self as NetConnection>::read_timeout(self)
    }

    fn write_timeout(&self) -> io::Result<Option<Duration>> {
        <Self as NetConnection>::write_timeout(self)
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        <Self as NetConnection>::set_read_timeout(self, dur)
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        <Self as NetConnection>::set_write_timeout(self, dur)
    }

    fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()> {
        <Self as NetConnection>::set_nonblocking(self, nonblocking)
    }
}

#[cfg(feature = "socket2")]
impl NetSession for socket2::Socket {
    type Context = ();
    type Connection = Self;
    type RemoteAddr = net::SocketAddr;
    type PeerAddr = net::SocketAddr;

    fn accept(connection: Self::Connection, _context: &Self::Context) -> Self {
        connection
    }

    fn connect(addr: Self::RemoteAddr, _context: &Self::Context) -> io::Result<Self> {
        Self::connect_nonblocking(addr)
    }

    fn peer_addr(&self) -> Self::PeerAddr {
        <Self as NetConnection>::peer_addr(self)
    }

    fn read_timeout(&self) -> io::Result<Option<Duration>> {
        <Self as NetConnection>::read_timeout(self)
    }

    fn write_timeout(&self) -> io::Result<Option<Duration>> {
        <Self as NetConnection>::write_timeout(self)
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        <Self as NetConnection>::set_read_timeout(self, dur)
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        <Self as NetConnection>::set_write_timeout(self, dur)
    }

    fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()> {
        <Self as NetConnection>::set_nonblocking(self, nonblocking)
    }
}