use anyhow::{Context, Result};
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    vec,
};
use walkdir::WalkDir;

const PROTOS: &str = "proto";

fn main() -> Result<()> {
    compile_protos()?;
    Ok(())
}

fn compile_protos() -> Result<()> {
    let protos = list_protos(Path::new(PROTOS))?;
    let mut config = prost_build::Config::new();
    config.bytes(["."]); // Use `Bytes` instead of `Vec<u8>` for PB type `bytes`.
    config
        .compile_protos(&protos, &[PROTOS])
        .context("Cannot compile protos")
}

fn list_protos(dir: &Path) -> Result<Vec<PathBuf>> {
    WalkDir::new(dir)
        .into_iter()
        .try_fold(vec![], |mut protos, entry| {
            let entry = entry.context("Cannot read directory entry")?;
            let path = entry.path();
            if path.extension().and_then(OsStr::to_str) == Some("proto") {
                protos.push(path.to_path_buf());
            }
            Ok(protos)
        })
}
