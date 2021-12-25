use std::process::Command;

use color_eyre::eyre::{ensure, Result};
use structopt::StructOpt;

use crate::{benchrunner_runtime_dir, escape_systemd_run_arg};

#[derive(StructOpt)]
pub struct Cmd {
    #[structopt(long, short)]
    threads: usize,
    #[structopt(long, short)]
    memory_mb: usize,
    #[structopt(long, short)]
    foreground: bool,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let executable = std::env::current_exe()?;
        let run_dir = benchrunner_runtime_dir()?;

        let status = Command::new("sudo")
            .arg("systemd-run")
            .arg("--unit=benchrunner")
            .arg("--collect")
            .args(if self.foreground {
                ["--pty", "--wait"].as_slice()
            } else {
                [].as_slice()
            })
            .arg("--")
            .arg(escape_systemd_run_arg(executable))
            .arg("daemon")
            .arg(format!("--uid={}", uid))
            .arg(format!("--gid={}", gid))
            .arg("--run-dir")
            .arg(escape_systemd_run_arg(run_dir))
            .arg(format!("--threads={}", self.threads))
            .arg(format!("--memory-mb={}", self.memory_mb))
            .args([
                "--limit=user.slice",
                "--limit=system.slice",
                "--limit=init.scope",
            ])
            .arg("--unit=benchrunner.service")
            .status()?;

        ensure!(status.success(), "systemd-run failed");

        Ok(())
    }
}
