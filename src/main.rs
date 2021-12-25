use std::{
    ffi::{OsStr, OsString},
    os::unix::prelude::{OsStrExt, OsStringExt},
    path::PathBuf,
};

use color_eyre::{eyre::Result, Report};
use structopt::StructOpt;

mod daemon;
mod run;
mod start;
mod stats;

mod protocol;

fn fail(err: Report) -> ! {
    eprintln!("{:?}", err);
    std::process::exit(1);
}

fn benchrunner_runtime_dir() -> Result<PathBuf> {
    if let Some(dir) = std::env::var_os("BENCHRUNNER_RUNTIME_DIR") {
        return Ok(dir.into());
    }

    if let Some(dirs) = directories::BaseDirs::new() {
        if let Some(runtime_dir) = dirs.runtime_dir() {
            return Ok(runtime_dir.join("benchrunner"));
        }
    }

    Err(color_eyre::eyre::eyre!(
        "could not detect a suitable runtime directory, set BENCHRUNNER_RUNTIME_DIR"
    ))
}

fn escape_systemd_run_arg(arg: impl AsRef<OsStr>) -> OsString {
    OsString::from_vec(
        arg.as_ref()
            .as_bytes()
            .iter()
            .flat_map(|&c| [c].into_iter().chain((c == b'$').then(|| b'$')))
            .collect(),
    )
}

#[derive(StructOpt)]
enum Cmd {
    Daemon(daemon::Cmd),
    Run(run::Cmd),
    Start(start::Cmd),
    Stats(stats::Cmd),
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        match self {
            Cmd::Daemon(cmd) => cmd.run(),
            Cmd::Run(cmd) => cmd.run(),
            Cmd::Start(cmd) => cmd.run(),
            Cmd::Stats(cmd) => cmd.run(),
        }
    }
}

fn main() -> Result<()> {
    color_eyre::install()?;

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").as_deref().unwrap_or("info"),
        ))
        .with_writer(std::io::stderr)
        .init();

    Cmd::from_args().run()
}
