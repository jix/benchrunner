use std::{
    collections::BTreeMap,
    io::{BufReader, BufWriter, Write},
    os::unix::net::UnixStream,
};

use color_eyre::eyre::Result;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{de::IoRead, Deserializer};

use super::benchrunner_runtime_dir;

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskDef {
    pub name: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub cleanup_command: Vec<String>,
    pub threads: usize,
    pub memory_mb: usize,
    #[serde(default)]
    pub watchdog: usize,
    #[serde(default)]
    pub cleanup_watchdog: usize,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub priority_group: i64,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskEvent {
    pub name: String,
    pub id: u64,

    pub event: Event,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Event {
    Scheduled,
    Started,
    Finished {
        status: Status,
        cleanup_status: Option<Status>,
    },
    Rejected,
    RunnerFailure {
        message: String,
    },
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::Scheduled => write!(f, "scheduled"),
            Event::Started => write!(f, "started"),
            Event::Finished {
                status,
                cleanup_status,
            } => {
                if let Some(cleanup_status) = cleanup_status {
                    write!(f, "{} (cleanup {})", status, cleanup_status)
                } else {
                    write!(f, "{}", status)
                }
            }
            Event::Rejected => write!(f, "rejected"),
            Event::RunnerFailure { message } => write!(f, "runner failure: {}", message),
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum Status {
    Exited { code: u32 },
    ExecutableNotFound,
    OutOfMemory,
    WatchdogTimeout,
    Killed,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Exited { code } => {
                write!(f, "exit code {}", code)
            }
            Status::ExecutableNotFound => write!(f, "executable not found"),
            Status::OutOfMemory => write!(f, "out of memory"),
            Status::WatchdogTimeout => write!(f, "watchdog timeout"),
            Status::Killed => write!(f, "killed"),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct Stats {
    pub running: usize,
    pub pending: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Pong {
    Pong,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Action {
    AddTasks { tasks: Vec<TaskDef> },
    Stats,
    Ping,
}

pub struct Connection {
    read: Deserializer<IoRead<BufReader<UnixStream>>>,
    write: BufWriter<UnixStream>,
}

impl Connection {
    pub fn new(stream: UnixStream) -> Result<Self> {
        let read = Deserializer::from_reader(BufReader::new(stream.try_clone()?));
        let write = BufWriter::new(stream);

        Ok(Self { read, write })
    }

    pub fn connect() -> Result<Self> {
        Self::new(UnixStream::connect(
            benchrunner_runtime_dir()?.join("benchrunner.sock"),
        )?)
    }

    pub fn read<T>(&mut self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        Ok(T::deserialize(&mut self.read)?)
    }

    pub fn try_read<T>(&mut self) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let data = T::deserialize(&mut self.read);
        match data {
            Err(err) if err.is_eof() => Ok(None),
            data => Ok(Some(data?)),
        }
    }

    pub fn write<T>(&mut self, data: &T) -> Result<()>
    where
        T: Serialize,
    {
        serde_json::to_writer(&mut self.write, data)?;
        self.write.write_all(b"\n")?;
        self.write.flush()?;
        Ok(())
    }

    pub fn write_ok<T>(&mut self, data: &T) -> Result<()>
    where
        T: Serialize,
    {
        self.write(&Ok::<_, ()>(data))
    }

    pub fn write_err<T>(&mut self, data: &T) -> Result<()>
    where
        T: ToString,
    {
        self.write(&Err::<(), String>(data.to_string()))
    }

    pub fn try_read_result<T>(&mut self) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.try_read::<Result<T, String>>()?
            .transpose()
            .map_err(|err| color_eyre::eyre::eyre!("error response: {}", err))
    }
}
