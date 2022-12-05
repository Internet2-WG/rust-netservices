#[macro_use]
extern crate amplify;

#[cfg(feature = "re-actor")]
pub mod actors;

#[cfg(feature = "poll-reactor")]
pub mod wire;

mod connection;
mod frame;
mod listener;
pub mod noise;
mod session;

pub use connection::NetConnection;
pub use frame::{Frame, VecFrame};
pub use listener::NetListener;
pub use session::NetSession;

pub trait IoStream: std::io::Write + std::io::Read {}

impl<T> IoStream for T where T: std::io::Write + std::io::Read {}