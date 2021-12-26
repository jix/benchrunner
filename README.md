# Benchrunner

A small daemon for running benchmarks on my home workstation. Requires systemd.

Written with only my personal use in mind. There's no proper documentation, not
much testing done, no stability guarantee, the daemon needs to run as root and
there probably are some security issues. Nevertheless, feel free to use and
adapt it for your own needs, as long as you don't expect support from me.

## Features

Tasks are enqueued by submitting them over a unix socket (e.g. by using the
`run` command of the CLI).

Each task's command is run in an individual transient systemd service. This
service is used to limit resources (CPU threads, memory, wall time) and to
setup an isolated `/tmp` tmpfs that is automatically cleared. It is also used
to dissuade the OOM killer from targeting a task _unless_ that task exceeds its
own memory limit, i.e. to prevent a non-benchrunner program from accidentally
taking down benchrunner tasks.

A task is pinned to a fixed set of CPU threads. If a task uses an even number
of CPU threads, they are allocated such that they do not share cores on 2-way
SMT systems. Additionally `user.slice`, `system.slice` and `init.scope` are
excluded from running on a CPU thread that is currently running a task. This is
useful to reduce noise when benchmarking, especially when running on a system
where the background load varies from time to time, e.g. a desktop machine that
you're using.[^1] Of course there is still noise coming from other shared
resources, e.g. memory bandwidth and some levels of cache, for which all
concurrently running processes are competing.

A task can also have an optional cleanup command, which is also run if the main
command fails, e.g. due to running out of memory. The working directory for the
tasks is a temporary directory that persists between the main and cleanup
command. This can be used to process the results of an interrupted task.

The `stats` command connects to the daemon and outputs the number of currently
running and queued tasks every time one of them changes.

The `start` command starts the daemon as a systemd service. It requires you to
specify the maximal resources (threads and memory) to be used by benchrunner.

**WARNING:** you need to specify a memory limit that is _below_ your current
(and usual) amount of free memory or bad things may happen. Benchrunner expects
that it is free to use the specified amount at all times and will cause the OOM
killer to kill non-benchrunner processes if it can't.

The `start` command uses `sudo` and `systemd-run` to create a transient system
service for the daemon. While the daemon needs to run as root to control
systemd and the tasks are also system services, the task's commands are run as
the user running the `start` command.

Optionally `-f` can be used with the `start` command to keep the daemon in the
foreground in the current terminal.

While the `daemon` command can be run directly, that's not recommended. When it
isn't running as a systemd service, there is no simple way to stop it
_including_ all currently running tasks.

## Missing Features

Right now, the scheduling is a naive priority queue sorted by the tuple
`(priority_group, threads, priority)` that blocks as soon as the resources for
the highest priority pending task are in use. It will also never schedule two
tasks using a different number of threads at the same time. This is ok as long
as all enqueued tasks have the same resource requirements, but can be very
impractical when they don't. While I don't need this for my current main use
case, it is something that I'd like to eventually improve.

There is no easy way to stop individual running tasks, using systemctl to stop
a task should work, but I haven't tested that. There is also no way to remove
pending tasks. Right now the only way to stop all tasks and to empty the queue
is to stop the daemon using systemctl or, when running it with `-f`, by
interrupting it.

## Example Use Case

I'm using this at https://github.com/jix/satbench, which is also just for my
personal use and currently an early work in progress, but might still help if
you want to figure out how this can be used.

[^1]: When benchmarking on a desktop machine, you most likely also want to
disable thermally limited dynamic frequency scaling. Usually, this can be done
by writing `0` to `/sys/devices/system/cpu/cpufreq/boost`. On a laptop, though,
even that is often not sufficient. There, getting long-term consistent
performance under load may require further reduction of the CPU frequency.
