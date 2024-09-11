// Library for building scalable privacy-preserving microservices P2P nodes
//
// SPDX-License-Identifier: Apache-2.0
//
// Written in 2022-2024 by
//     Dr. Maxim Orlovsky <orlovsky@cyphernet.org>
//
// Copyright 2022-2024 Cyphernet Labs, IDCS, Switzerland
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{io, thread};
use std::any::Any;
use reactor::poller::popol;
use reactor::Reactor;
use crate::{Artifact, NetAccept, NetConnection, NetListener, NetSession, NodeId};

/// The client runtime containing reactor thread managing connection to the remote server and the
/// use of the server APIs.
pub struct Server {
    reactor: Reactor<ServerCtl, popol::Poller>, /* seems we do not need to pass any commands to
                                               * the
                                               * reactor */
}

impl Server {
    /// Constructs new client for client-server protocol. Takes service callback delegate and remote
    /// server address. Will attempt to connect to the server automatically once the reactor thread
    /// has started.
    pub fn new<
        S: NetSession + 'static,
        L: NetListener<Stream = S::Connection> + 'static,
        C: NodeController<Addr, S, L> + 'static,
    >(
        node_id: <S::Artifact as Artifact>::NodeId,
        delegate: C,
        listen: impl IntoIterator<Item = NetAccept<S, L>>,
    ) -> io::Result<Self> {
        let mut service = NodeService::<S, L, C>::new(node_id, delegate);
        for socket in listen {
            service.listen(socket);
        }
        let reactor = Reactor::named(service, popol::Poller::new(), s!("node-service"))?;
        Ok(Self { reactor })
    }

    /// Joins the reactor thread.
    pub fn join(self) -> thread::Result<()> { self.reactor.join() }

    /// Terminates the node, closing all connections, unbinding listeners and stopping the reactor
    /// thread.
    pub fn terminate(self) -> Result<(), Box<dyn Any + Send>> {
        self.reactor
            .controller()
            .cmd(ServerCtl::Terminate)
            .map_err(|err| Box::new(err) as Box<dyn Any + Send>)?;
        self.reactor.join()?;
        Ok(())
    }
}
