fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Keep proto compilation self-contained across developer machines and CI.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["../../proto/coordination.proto"], &["../../proto"])?;

    Ok(())
}
