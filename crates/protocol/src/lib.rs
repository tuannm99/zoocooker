pub mod command;
pub mod error;
pub mod path;
pub mod types;

pub mod proto {
    tonic::include_proto!("zoocooker.v1");
}
