use std::{
    cmp::Reverse,
    collections::{BTreeSet, BinaryHeap},
    io::ErrorKind,
    path::PathBuf,
    sync::{mpsc, Arc, Mutex},
    thread::Thread,
};

use color_eyre::eyre::{bail, ensure, Result};
use structopt::StructOpt;

use crate::{
    escape_systemd_run_arg, fail,
    protocol::{Action, Connection, Event, Pong, Stats, Status, TaskDef, TaskEvent},
};

#[derive(StructOpt)]
pub struct Cmd {
    #[structopt(long)]
    uid: u32,
    #[structopt(long)]
    gid: u32,
    #[structopt(long)]
    run_dir: PathBuf,
    #[structopt(long)]
    unit: Option<String>,
    #[structopt(long)]
    threads: usize,
    #[structopt(long)]
    memory_mb: usize,
    #[structopt(long)]
    limit: Vec<String>,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        ensure!(unsafe { libc::getuid() } == 0, "agent needs to run as root");

        unsafe {
            let mut rlimit = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            ensure!(
                libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlimit) == 0,
                "getrlimit failed"
            );

            rlimit.rlim_cur = rlimit.rlim_max;

            ensure!(
                libc::setrlimit(libc::RLIMIT_NOFILE, &rlimit) == 0,
                "setrlimit failed"
            );
        }

        unsafe { libc::umask(0o077) };
        unsafe {
            ensure!(libc::setegid(self.gid) == 0, "setegid failed");
            ensure!(libc::seteuid(self.uid) == 0, "seteuid failed");
        }

        std::fs::create_dir_all(&self.run_dir)?;

        let socket = self.run_dir.join("benchrunner.sock");

        match std::fs::remove_file(&socket) {
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            res => res,
        }?;

        let socket = std::os::unix::net::UnixListener::bind(&socket)?;

        unsafe {
            ensure!(libc::seteuid(0) == 0, "seteuid failed (undo)");
            ensure!(libc::setegid(0) == 0, "setegid failed (undo)");
        }

        let daemon = Arc::new(Mutex::new(Daemon::new(Config {
            uid: self.uid,
            gid: self.gid,
            max_threads: self.threads,
            max_memory_mb: self.memory_mb,
            limit: self.limit,
            unit: self.unit,
            run_dir: self.run_dir,
        })?));

        {
            let daemon = daemon.clone();
            std::thread::spawn(move || loop {
                match socket.accept() {
                    Ok((connection, _peer)) => {
                        let daemon = daemon.clone();
                        std::thread::spawn(move || {
                            #[allow(clippy::redundant_closure)]
                            let mut connection =
                                Connection::new(connection).unwrap_or_else(|err| fail(err));
                            if let Err(err) = handle_connection(daemon, &mut connection) {
                                tracing::error!(%err);
                                let _ = connection.write_err(&err);
                            }
                        });
                    }
                    Err(err) => fail(err.into()),
                }
            });
        }

        while Daemon::step(&daemon) {
            std::thread::park();
        }

        Ok(())
    }
}

fn handle_connection(daemon: Arc<Mutex<Daemon>>, connection: &mut Connection) -> Result<()> {
    match connection.read::<Action>()? {
        Action::AddTasks { tasks } => {
            let receiver = Daemon::add_tasks(&daemon, tasks)?;
            while let Ok(event) = receiver.recv() {
                connection.write_ok(&event)?;
            }
        }
        Action::Stats => {
            let receiver = Daemon::listen_stats(&daemon)?;
            while let Ok(stats) = receiver.recv() {
                connection.write_ok(&stats)?;
            }
        }
        Action::Ping => {
            connection.write_ok(&Pong::Pong)?;
        }
    }

    Ok(())
}

struct Task {
    id: u64,
    def: TaskDef,
    done: mpsc::Sender<TaskEvent>,
}

impl Task {
    fn priority_tuple(&self) -> (i64, usize, i64, Reverse<u64>) {
        (
            self.def.priority_group,
            self.def.threads,
            self.def.priority,
            Reverse(self.id),
        )
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.priority_tuple() == other.priority_tuple()
    }
}

impl Eq for Task {}

impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority_tuple().cmp(&other.priority_tuple())
    }
}

struct Config {
    max_threads: usize,
    max_memory_mb: usize,

    limit: Vec<String>,

    uid: u32,
    gid: u32,

    unit: Option<String>,

    run_dir: PathBuf,
}

struct Daemon {
    thread: Thread,
    pending: BinaryHeap<Task>,

    counter: u64,

    last_thread: usize,

    config: Arc<Config>,

    memory_mb_available: usize,

    slot_size: Option<usize>,
    free_slots: BTreeSet<usize>,
    running_slots: BTreeSet<usize>,

    reclaimed_memory_mb: usize,
    reclaimed_slots: Vec<usize>,

    start_tasks: Vec<(Task, usize)>,

    last_stats: Stats,
    stats_listeners: Vec<mpsc::Sender<Stats>>,
}

impl Daemon {
    fn new(config: Config) -> Result<Self> {
        let all_cpus = std::fs::read_to_string("/sys/fs/cgroup/cpuset.cpus.effective")?;

        let last_thread = if let Some(last_thread) = all_cpus.trim().strip_prefix("0-") {
            last_thread.parse()?
        } else {
            bail!("unexpected cpuset.cpus.effective");
        };

        let system_threads = last_thread + 1;

        if config.limit.is_empty() {
            ensure!(config.max_threads <= system_threads, "not enough threads");
        } else {
            ensure!(
                config.max_threads < system_threads,
                "no non-limited thread left"
            );
        }

        let memory_mb_available = config.max_memory_mb;
        Ok(Self {
            thread: std::thread::current(),
            pending: Default::default(),
            counter: 0,
            last_thread,
            config: Arc::new(config),
            memory_mb_available,
            slot_size: None,
            free_slots: BTreeSet::new(),
            running_slots: BTreeSet::new(),

            reclaimed_memory_mb: 0,
            reclaimed_slots: vec![],

            start_tasks: vec![],

            last_stats: Stats {
                running: 0,
                pending: 0,
            },
            stats_listeners: vec![],
        })
    }

    fn step(me: &Arc<Mutex<Self>>) -> bool {
        let mut locked = me.lock().unwrap();
        locked.step_locked(me)
    }

    fn step_locked(&mut self, me: &Arc<Mutex<Self>>) -> bool {
        let initial_free = self.unlimited_slots();

        self.memory_mb_available += self.reclaimed_memory_mb;
        self.reclaimed_memory_mb = 0;

        for slot in self.reclaimed_slots.drain(..) {
            self.running_slots.remove(&slot);
            self.free_slots.insert(slot);
        }

        if self.running_slots.is_empty() {
            self.slot_size = None;
            self.free_slots.clear();
        }

        while self.step_once() {}

        let final_free = self.unlimited_slots();

        if initial_free != final_free {
            tracing::debug!("changing limit");

            if let Err(err) = self.set_limit(final_free) {
                fail(err);
            }
        }

        if !self.start_tasks.is_empty() {
            tracing::debug!(count = self.start_tasks.len(), "launching");
        }

        for (task, slot) in self.start_tasks.drain(..) {
            tracing::debug!(slot, name = %task.def.name, id = %task.id, "launching");

            {
                let me = me.clone();
                let config = self.config.clone();
                std::thread::spawn(move || {
                    let memory_mb = task.def.memory_mb;
                    Self::run_task(&config, task, slot);
                    Self::make_available(&me, memory_mb, slot);
                });
            }
        }

        self.send_current_stats();

        true
    }

    fn unlimited_slots(&self) -> BTreeSet<usize> {
        if let Some(slot_size) = self.slot_size {
            let mut slots = BTreeSet::new();

            for slot in &self.free_slots {
                slots.extend(slot * slot_size..(slot + 1) * slot_size);
            }

            slots.extend((self.config.max_threads / slot_size) * slot_size..=self.last_thread);
            slots
        } else {
            (0..=self.last_thread).collect()
        }
    }

    fn step_once(&mut self) -> bool {
        let next = if let Some(next) = self.pending.peek() {
            next
        } else {
            return false;
        };

        if self.memory_mb_available < next.def.memory_mb {
            return false;
        }

        if let Some(slot_size) = self.slot_size {
            if slot_size != next.def.threads {
                return false;
            }
        } else {
            self.slot_size = Some(next.def.threads);
            tracing::debug!(threads = %next.def.threads, "setting slot_size");
            self.free_slots
                .extend(0..self.config.max_threads / next.def.threads);
        };

        let slot = if let Some(&slot) = self.free_slots.iter().next() {
            slot
        } else {
            return false;
        };

        let next = self.pending.pop().unwrap();

        self.memory_mb_available -= next.def.memory_mb;
        self.free_slots.remove(&slot);
        self.running_slots.insert(slot);

        self.start_tasks.push((next, slot));

        true
    }

    fn add_tasks(me: &Arc<Mutex<Self>>, tasks: Vec<TaskDef>) -> Result<mpsc::Receiver<TaskEvent>> {
        let mut locked = me.lock().unwrap();
        let result = locked.add_tasks_locked(tasks);
        let thread = locked.thread.clone();
        drop(locked);
        thread.unpark();
        result
    }

    fn add_tasks_locked(&mut self, tasks: Vec<TaskDef>) -> Result<mpsc::Receiver<TaskEvent>> {
        let (sender, receiver) = mpsc::channel();

        for def in tasks {
            self.counter += 1;
            let task = Task {
                id: self.counter,
                def,
                done: sender.clone(),
            };
            if !(1..=self.config.max_threads).contains(&task.def.threads)
                || !(1..=self.config.max_memory_mb).contains(&task.def.memory_mb)
                || task.def.command.is_empty()
            {
                let _ = task.done.send(TaskEvent {
                    name: task.def.name.clone(),
                    id: task.id,
                    event: Event::Rejected,
                });
            } else {
                let _ = task.done.send(TaskEvent {
                    name: task.def.name.clone(),
                    id: task.id,
                    event: Event::Scheduled,
                });
                self.pending.push(task);
            }
        }

        Ok(receiver)
    }

    fn listen_stats(me: &Arc<Mutex<Self>>) -> Result<mpsc::Receiver<Stats>> {
        let mut locked = me.lock().unwrap();
        let result = locked.listen_stats_locked();
        let thread = locked.thread.clone();
        drop(locked);
        thread.unpark();
        result
    }

    fn listen_stats_locked(&mut self) -> Result<mpsc::Receiver<Stats>> {
        let (sender, receiver) = mpsc::channel();

        let _ = sender.send(self.current_stats());
        self.stats_listeners.push(sender);

        Ok(receiver)
    }

    fn current_stats(&self) -> Stats {
        Stats {
            running: self.running_slots.len(),
            pending: self.pending.len(),
        }
    }

    fn send_current_stats(&mut self) {
        let stats = self.current_stats();
        if self.last_stats == stats {
            return;
        }
        self.last_stats = stats;
        self.stats_listeners
            .retain(|listener| listener.send(stats).is_ok());
    }

    fn run_task(config: &Arc<Config>, task: Task, slot: usize) {
        let _ = task.done.send(TaskEvent {
            name: task.def.name.clone(),
            id: task.id,
            event: Event::Started,
        });

        let result = Self::run_task_fallible(config, &task, slot, None);

        let mut cleanup_result = None;

        if !task.def.cleanup_command.is_empty() {
            if let Ok(status) = result {
                cleanup_result = Some(Self::run_task_fallible(config, &task, slot, Some(status)));
            }
        }

        let _ = Self::cleanup_task_dir(config, &task);

        match (result, cleanup_result.transpose()) {
            (Err(err), _) | (_, Err(err)) => {
                tracing::error!(
                    slot,
                    name = %task.def.name,
                    id = %task.id,
                    error = %err,
                    "runner failure",
                );

                let _ = task.done.send(TaskEvent {
                    name: task.def.name.clone(),
                    id: task.id,
                    event: Event::RunnerFailure {
                        message: err.to_string(),
                    },
                });
            }
            (Ok(status), Ok(cleanup_status)) => {
                tracing::debug!(slot, name = %task.def.name, id = %task.id, "task finished");

                let _ = task.done.send(TaskEvent {
                    name: task.def.name.clone(),
                    id: task.id,
                    event: Event::Finished {
                        status,
                        cleanup_status,
                    },
                });
            }
        }
    }

    fn run_as_user(config: &Arc<Config>) -> std::process::Command {
        let mut cmd = std::process::Command::new("setpriv");

        cmd.arg(format!("--reuid={}", config.uid))
            .arg(format!("--regid={}", config.gid))
            .arg("--inh-caps=-all")
            .arg("--init-groups")
            .arg("--");

        cmd
    }

    fn task_dir(config: &Arc<Config>, task: &Task) -> PathBuf {
        config.run_dir.join(format!("task-{}", task.id))
    }

    fn cleanup_task_dir(config: &Arc<Config>, task: &Task) -> Result<PathBuf> {
        let task_dir = Self::task_dir(config, task);

        let status = Self::run_as_user(config)
            .args(&["rm", "-rf", "--one-file-system", "--"])
            .arg(&task_dir)
            .status()?;

        ensure!(status.success(), "cleanup task dir failed");

        Ok(task_dir)
    }

    fn make_task_dir(config: &Arc<Config>, task: &Task) -> Result<PathBuf> {
        let task_dir = Self::cleanup_task_dir(config, task)?;

        let status = Self::run_as_user(config)
            .args(&["mkdir", "--"])
            .arg(&task_dir)
            .status()?;

        ensure!(status.success(), "make task dir failed");

        Ok(task_dir)
    }

    fn run_task_fallible(
        config: &Arc<Config>,
        task: &Task,
        slot: usize,
        cleanup: Option<Status>,
    ) -> Result<Status> {
        let threads = task.def.threads * slot..task.def.threads * (slot + 1);

        let task_dir = if cleanup.is_none() {
            Self::make_task_dir(config, task)?
        } else {
            Self::task_dir(config, task)
        };

        let output = std::process::Command::new("systemd-run")
            .arg(format!("--uid={}", config.uid))
            .arg(format!("--gid={}", config.gid))
            .arg("--wait")
            .arg("--same-dir")
            .arg("--collect")
            .arg("--slice=benchrunner")
            .arg(format!(
                "--unit=benchrunner-task-{}-{}",
                config.uid, task.id
            ))
            .arg(format!(
                "--property=AllowedCPUs={}",
                threads
                    .map(|thread| thread.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ))
            .arg(format!("--property=MemoryMax={}M", task.def.memory_mb))
            .arg(format!(
                "--property=WatchdogSec={}",
                if cleanup.is_none() {
                    task.def.watchdog
                } else {
                    task.def.cleanup_watchdog
                }
            ))
            .arg("--property=TimeoutAbortSec=0")
            .arg("--property=NotifyAccess=all")
            .arg("--property=PrivateTmp=yes")
            .arg("--property=OOMScoreAdjust=-998")
            .args(
                config
                    .unit
                    .as_ref()
                    .map(|unit| format!("--property=BindsTo={}", unit)),
            )
            .arg(format!("--setenv=BENCHRUNNER_TASK={}", task.id))
            .arg(format!("--setenv=BENCHRUNNER_NAME={}", task.def.name))
            .args(cleanup.is_some().then(|| "--setenv=BENCHRUNNER_CLEANUP=1"))
            .args(
                cleanup
                    .as_ref()
                    .map(|status| format!("--setenv=BENCHRUNNER_STATUS={}", status)),
            )
            .args(
                task.def
                    .env
                    .iter()
                    .map(|(var, value)| format!("--setenv={}={}", var, value)),
            )
            .arg("--")
            .arg("/usr/bin/env")
            .arg("--")
            .args(
                if cleanup.is_none() {
                    &task.def.command
                } else {
                    &task.def.cleanup_command
                }
                .iter()
                .map(escape_systemd_run_arg),
            )
            .current_dir(task_dir)
            .output()?;

        let output = std::str::from_utf8(&output.stderr)?;

        let mut result = String::new();
        let mut terminated = String::new();
        let mut executable_not_found = false;

        for line in output.lines() {
            if let Some(matched) = line.strip_prefix("Finished with result: ") {
                result = matched.to_string();
            } else if let Some(matched) = line.strip_prefix("Main processes terminated with: ") {
                terminated = matched.to_string();
            } else if line.starts_with("Failed to find executable ") {
                executable_not_found = true;
            }
        }

        if executable_not_found {
            return Ok(Status::ExecutableNotFound);
        }

        match &*result {
            "success" | "exit-code" => {
                if let Some(matched) = terminated.strip_prefix("code=exited/status=") {
                    if let Ok(code) = matched.parse() {
                        return Ok(Status::Exited { code });
                    }
                } else if terminated.starts_with("code=killed/status=") {
                    return Ok(Status::Killed);
                }
            }
            "oom-kill" => {
                return Ok(Status::OutOfMemory);
            }
            "watchdog" => {
                return Ok(Status::WatchdogTimeout);
            }
            _ => (),
        }

        bail!(
            "could not determine task exit status ({:?} {:?} {:?})",
            result,
            terminated,
            output
        );
    }

    fn make_available(me: &Arc<Mutex<Self>>, memory_mb: usize, slot: usize) {
        let mut locked = me.lock().unwrap();
        locked.make_available_locked(memory_mb, slot);
        let thread = locked.thread.clone();
        drop(locked);
        thread.unpark();
    }

    fn make_available_locked(&mut self, memory_mb: usize, slot: usize) {
        self.reclaimed_memory_mb += memory_mb;
        self.reclaimed_slots.push(slot);
    }

    fn set_limit(&self, unlimited_slots: BTreeSet<usize>) -> Result<()> {
        for target in &self.config.limit {
            let status = std::process::Command::new("systemctl")
                .arg("set-property")
                .arg("--runtime")
                .arg(target)
                .arg(format!(
                    "AllowedCPUs={}",
                    unlimited_slots
                        .iter()
                        .map(|thread| thread.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                ))
                .status()?;
            ensure!(status.success(), "systemctl failed");
        }

        Ok(())
    }
}
