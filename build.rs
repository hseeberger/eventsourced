#[cfg(feature = "nats")]
use anyhow::Context;
use anyhow::Result;
#[cfg(feature = "nats")]
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    vec,
};
#[cfg(feature = "nats")]
use walkdir::WalkDir;

#[cfg(feature = "nats")]
const PROTOS: &str = "proto";

fn main() -> Result<()> {
    #[cfg(feature = "nats")]
    compile_protos()?;

    Ok(())
}

#[cfg(feature = "nats")]
fn compile_protos() -> Result<()> {
    let protos = list_protos(Path::new(PROTOS))?;
    prost_build::compile_protos(&protos, &[PROTOS]).context("Cannot compile protos")
}

#[cfg(feature = "nats")]
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
