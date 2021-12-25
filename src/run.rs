use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
};

use color_eyre::eyre::Result;
use serde::Deserialize;
use serde_json::Deserializer;
use structopt::StructOpt;

use crate::protocol::{Action, Connection, TaskDef, TaskEvent};

#[derive(StructOpt)]
pub struct Cmd {
    task_file: Option<PathBuf>,
    #[structopt(long, short)]
    json_log: Option<PathBuf>,
    #[structopt(long, short, default_value = "0")]
    priority_group_offset: i64,
    #[structopt(long, short)]
    env: Vec<String>,
    #[structopt(long, short = "x")]
    exit_code: bool,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        let input = if let Some(task_file) = self.task_file {
            Box::new(File::open(task_file)?) as Box<dyn Read>
        } else {
            Box::new(std::io::stdin()) as Box<dyn Read>
        };
        let mut input = Deserializer::from_reader(BufReader::new(input));

        let mut env = vec![];

        for assignment in self.env {
            if let Some((var, value)) = assignment.split_once('=') {
                env.push((var.to_string(), value.to_string()));
            } else {
                env.push((assignment.to_string(), "1".to_string()))
            }
        }

        let mut log_file = self
            .json_log
            .map(|log_file| {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(log_file)
                    .map(BufWriter::new)
            })
            .transpose()?;

        let mut tasks = vec![];
        loop {
            match TaskDef::deserialize(&mut input) {
                Err(err) if err.is_eof() => break,
                result => tasks.push(result?),
            }
        }

        for task in &mut tasks {
            task.priority_group = task
                .priority_group
                .saturating_add(self.priority_group_offset);
            for (var, value) in env.iter() {
                task.env
                    .entry(var.to_string())
                    .or_insert_with(|| value.to_string());
            }
        }

        let mut pending = tasks.len();

        let mut connection = Connection::connect()?;

        connection.write(&Action::AddTasks { tasks })?;

        let mut all_ok = true;

        while let Some(event) = connection.try_read_result::<TaskEvent>()? {
            if let Some(log_file) = &mut log_file {
                serde_json::to_writer(&mut *log_file, &event)?;
                log_file.write_all(b"\n")?;
                log_file.flush()?;
            }

            #[derive(PartialEq, Eq, PartialOrd, Ord)]
            enum Level {
                Debug,
                Info,
                Warn,
                Error,
            }

            let mut level = Level::Info;

            match event.event {
                crate::protocol::Event::Scheduled => level = Level::Debug,
                crate::protocol::Event::Started => level = Level::Info,
                crate::protocol::Event::Finished {
                    status,
                    cleanup_status,
                } => {
                    pending -= 1;

                    for status in Some(status).iter().chain(cleanup_status.iter()) {
                        match status {
                            crate::protocol::Status::Exited { code } => {
                                if *code != 0 {
                                    level = level.max(Level::Warn);
                                    all_ok = false;
                                }
                            }
                            crate::protocol::Status::WatchdogTimeout
                            | crate::protocol::Status::OutOfMemory => {
                                level = level.max(Level::Warn);
                                all_ok = false;
                            }
                            crate::protocol::Status::ExecutableNotFound
                            | crate::protocol::Status::Killed => {
                                level = level.max(Level::Error);
                                all_ok = false;
                            }
                        }
                    }
                }
                crate::protocol::Event::Rejected | crate::protocol::Event::RunnerFailure { .. } => {
                    all_ok = false;
                    level = level.max(Level::Error)
                }
            }

            match level {
                Level::Error => {
                    tracing::error!(
                        name = %event.name,
                        id = event.id,
                        "[{}] {}",
                        pending,
                        event.event,
                    )
                }
                Level::Warn => {
                    tracing::warn!(
                        name = %event.name,
                        id = event.id,
                        "[{}] {}",
                        pending,
                        event.event,
                    )
                }
                Level::Info => {
                    tracing::info!(
                        name = %event.name,
                        id = event.id,
                        "[{}] {}",
                        pending,
                        event.event,
                    )
                }
                Level::Debug => {
                    tracing::debug!(
                        name = %event.name,
                        id = event.id,
                        "[{}] {}",
                        pending,
                        event.event,
                    )
                }
            }
        }

        if self.exit_code && !all_ok {
            std::process::exit(1);
        }

        Ok(())
    }
}
