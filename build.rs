fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Build scripts run before any application thread exists.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .bytes(".")
        .compile_protos(&["proto/pulse.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/pulse.proto");
    println!("cargo:rerun-if-changed=third_party/nitro/dictionary.bin");
    Ok(())
}
