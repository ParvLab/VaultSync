use std::sync::{Arc, Mutex};
use std::time::Duration;
use futures::StreamExt;
use libp2p::{
    gossipsub,
    mdns,
    identify,
    swarm::{SwarmEvent, NetworkBehaviour},
    Swarm,
    SwarmBuilder,
    Multiaddr,
};
use tokio::sync::mpsc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingMutation {
    pub id: String,
    pub namespace: String,
    pub sequence: u64,
    pub doc_id: String,
    pub record_id: String,
    pub encrypted_blob: Vec<u8>,
    pub timestamp: u64,
    pub key_version: u64,
}

#[derive(Debug, Clone)]
pub enum Command {
    Broadcast(PendingMutation),
    Dial(Multiaddr),
}

#[derive(NetworkBehaviour)]
pub struct DriftBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub identify: identify::Behaviour,
}

#[derive(Clone)]
pub struct LibP2pTransportHandle {
    outgoing_tx: mpsc::Sender<Command>,
    listen_addresses: Arc<Mutex<Vec<Multiaddr>>>,
}

impl LibP2pTransportHandle {
    pub async fn broadcast_mutation(&self, mutation: PendingMutation) -> Result<(), String> {
        self.outgoing_tx.send(Command::Broadcast(mutation)).await.map_err(|e| e.to_string())
    }

    pub async fn dial(&self, addr: Multiaddr) -> Result<(), String> {
        self.outgoing_tx.send(Command::Dial(addr)).await.map_err(|e| e.to_string())
    }

    pub fn listen_addresses(&self) -> Vec<Multiaddr> {
        self.listen_addresses.lock().unwrap().clone()
    }
}

pub struct LibP2pTransport {
    namespace: String,
    swarm: Swarm<DriftBehaviour>,
    incoming_tx: mpsc::Sender<PendingMutation>,
    outgoing_rx: mpsc::Receiver<Command>,
    listen_addresses: Arc<Mutex<Vec<Multiaddr>>>,
}

impl LibP2pTransport {
    pub async fn new(
        namespace: &str,
        listen_addr: Option<String>,
        incoming_tx: mpsc::Sender<PendingMutation>,
    ) -> Result<(Self, LibP2pTransportHandle), String> {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )
            .map_err(|e| format!("Failed to configure TCP transport: {:?}", e))?
            .with_quic()
            .with_behaviour(|key| {
                let gossip_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(1))
                    .validation_mode(gossipsub::ValidationMode::Anonymous)
                    .build()
                    .map_err(|e| format!("Gossipsub config error: {:?}", e))?;

                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Anonymous,
                    gossip_config,
                )
                .map_err(|e| format!("Failed to create Gossipsub behaviour: {:?}", e))?;

                let mdns = mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    key.public().to_peer_id(),
                )
                .map_err(|e| format!("Failed to create mDNS behaviour: {:?}", e))?;

                let identify = identify::Behaviour::new(
                    identify::Config::new("/drift/1.0.0".to_string(), key.public()),
                );

                Ok(DriftBehaviour { gossipsub, mdns, identify })
            })
            .map_err(|e| format!("Failed to build behavior: {:?}", e))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        // Subscribe to namespace topic
        let topic_str = format!("drift/{}", namespace);
        let topic = gossipsub::IdentTopic::new(topic_str);
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&topic)
            .map_err(|e| format!("Failed to subscribe to gossipsub topic: {:?}", e))?;

        let actual_listen_addr = listen_addr.unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".to_string());
        let parsed_addr: Multiaddr = actual_listen_addr
            .parse()
            .map_err(|e| format!("Invalid listen address format: {:?}", e))?;

        swarm
            .listen_on(parsed_addr)
            .map_err(|e| format!("Failed to listen on address: {:?}", e))?;

        // Support QUIC too if listening on default
        if actual_listen_addr.contains("0.0.0.0/tcp/0") {
            if let Ok(quic_addr) = "/ip4/0.0.0.0/udp/0/quic-v1".parse::<Multiaddr>() {
                let _ = swarm.listen_on(quic_addr);
            }
        }

        let (outgoing_tx, outgoing_rx) = mpsc::channel(100);
        let listen_addresses = Arc::new(Mutex::new(Vec::new()));

        Ok((
            Self {
                namespace: namespace.to_string(),
                swarm,
                incoming_tx,
                outgoing_rx,
                listen_addresses: listen_addresses.clone(),
            },
            LibP2pTransportHandle {
                outgoing_tx,
                listen_addresses,
            },
        ))
    }

    pub async fn run(self) {
        let namespace = self.namespace;
        let incoming_tx = self.incoming_tx;
        let mut outgoing_rx = self.outgoing_rx;
        let mut swarm = self.swarm;
        let listen_addresses = self.listen_addresses;

        loop {
            tokio::select! {
                event = swarm.select_next_some() => {
                    match event {
                        SwarmEvent::Behaviour(DriftBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                            for (peer_id, addr) in list {
                                tracing::info!("P2P discovered peer: {} at {}", peer_id, addr);
                                swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                            }
                        }
                        SwarmEvent::Behaviour(DriftBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                            for (peer_id, addr) in list {
                                tracing::info!("P2P expired peer: {} at {}", peer_id, addr);
                                swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                            }
                        }
                        SwarmEvent::Behaviour(DriftBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                            propagation_source: _,
                            message_id: _,
                            message,
                        })) => {
                            if let Ok(m) = rmp_serde::from_slice::<PendingMutation>(&message.data) {
                                if m.namespace == namespace {
                                    tracing::debug!("P2P message received, mutation ID: {}", m.id);
                                    let _ = incoming_tx.send(m).await;
                                }
                            }
                        }
                        SwarmEvent::NewListenAddr { address, .. } => {
                            tracing::info!("P2P local node listening on {}", address);
                            listen_addresses.lock().unwrap().push(address);
                        }
                        _ => {}
                    }
                }
                Some(cmd) = outgoing_rx.recv() => {
                    match cmd {
                        Command::Broadcast(mutation) => {
                            if let Ok(bytes) = rmp_serde::to_vec(&mutation) {
                                let topic = gossipsub::IdentTopic::new(format!("drift/{}", namespace));
                                if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic, bytes) {
                                    tracing::error!("P2P publish failed: {:?}", e);
                                } else {
                                    tracing::debug!("P2P mutation broadcasted: {}", mutation.id);
                                }
                            }
                        }
                        Command::Dial(addr) => {
                            tracing::info!("P2P dialing explicit address: {}", addr);
                            if let Err(e) = swarm.dial(addr.clone()) {
                                tracing::error!("P2P dial failed: {:?}", e);
                            }
                        }
                    }
                }
            }
        }
    }
}
