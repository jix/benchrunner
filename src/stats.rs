use std::{io::ErrorKind, time::Duration};

use color_eyre::eyre::Result;
use structopt::StructOpt;

use crate::protocol::{Action, Connection, Stats};

#[derive(StructOpt)]
pub struct Cmd {
    #[structopt(long, short)]
    poll: bool,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        if !self.poll {
            Self::run_once()
        } else {
            loop {
                let result = Self::run_once();
                if let Err(err) = result {
                    if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
                        match io_err.kind() {
                            ErrorKind::ConnectionRefused | ErrorKind::NotFound => {
                                std::thread::sleep(Duration::from_secs(10));
                                continue;
                            }
                            _ => (),
                        }
                    }
                    return Err(err);
                }
            }
        }
    }

    fn run_once() -> Result<()> {
        let mut connection = Connection::connect()?;

        connection.write(&Action::Stats)?;

        while let Some(stats) = connection.try_read_result::<Stats>()? {
            println!("R: {} P: {}", stats.running, stats.pending);
        }

        Ok(())
    }
}
