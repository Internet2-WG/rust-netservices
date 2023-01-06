use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::RawFd;
use std::{io, net};

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Io {
    Read,
    Write,
}

pub trait ResourceId: Copy + Eq + Ord + Hash + Debug + Display {}

pub trait Resource: AsRawFd + io::Write + Send {
    type Id: ResourceId + Send;
    type Event;

    fn id(&self) -> Self::Id;

    fn handle_io(&mut self, io: Io) -> Option<Self::Event>;

    fn disconnect(self) -> io::Result<()>;
}

impl ResourceId for net::SocketAddr {}
impl ResourceId for RawFd {}