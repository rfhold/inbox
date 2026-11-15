pub mod pb {
    tonic::include_proto!("inbox.v1");
}
pub mod arrivals;
pub mod auth;
pub mod domains;
pub mod mcp;
pub mod mutation;
pub mod nats_config;
pub mod oauth;
pub mod pipeline;
pub mod service;
pub mod smtp;
pub mod telemetry;
pub mod validation;
