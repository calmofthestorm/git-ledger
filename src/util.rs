use std::ffi::OsString;
use std::path::Path;

use anyhow::Result;
use gix::Repository;
use gix_hash::ObjectId;
use gix_ref::{transaction::PreviousValue, Reference, Target};
use once_cell::sync::OnceCell;

pub fn init_repo(
    local_path: &Path,
    remote_spec: &str,
    remote_name: &str,
    retryable: bool,
) -> anyhow::Result<Repository> {
    // Gave up on trying to make this race-free. Probably not safe on untrusted
    // dirs in /tmp either.
    let repo = if local_path.exists() {
        gix::open(local_path)?
    } else {
        gix::init_bare(local_path)?
    };

    for _ in 0..20 {
        if repo.try_find_remote(remote_name).is_some() || retryable {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    match repo.try_find_remote(remote_name) {
        Some(..) => return Ok(repo),
        None if !retryable => {
            anyhow::bail!("Remote not found; unable to create");
        }
        None => {
            if !git_command()
                .current_dir(local_path)
                .arg("remote")
                .arg("add")
                .arg(remote_name)
                .arg(remote_spec)
                .status()?
                .success()
            {
                anyhow::bail!("a git command failed");
            }
            init_repo(local_path, remote_spec, remote_name, false)
        }
    }
}

pub fn is_ancestor(repo: &Repository, old: ObjectId, new: ObjectId) -> Result<bool> {
    for rev in repo.rev_walk([new]).all()? {
        if rev? == old {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn peeled_only(r: Option<Reference>) -> Result<Option<ObjectId>> {
    match r {
        None => Ok(None),
        Some(r) => match r.target {
            Target::Symbolic(..) => anyhow::bail!("Symbolic refs not supported"),
            Target::Peeled(id) => Ok(Some(id)),
        },
    }
}

pub fn fast_forward_reference<'r>(
    repo: &'r Repository,
    ref_name: &str,
    future_ref_name: &str,
) -> Result<bool> {
    let new = match peeled_only(repo.refs.try_find(future_ref_name)?)? {
        Some(id) => id,
        None => return Ok(true),
    };
    fast_forward(repo, ref_name, new)
}

pub fn fast_forward<'r>(repo: &'r Repository, ref_name: &str, id: ObjectId) -> Result<bool> {
    let cur_target = peeled_only(repo.refs.try_find(ref_name)?)?;

    match cur_target {
        None => {
            repo.reference(ref_name, id, PreviousValue::MustNotExist, "fast forward")?;
            Ok(true)
        }
        Some(cur) if cur == id => Ok(true),
        Some(cur) if is_ancestor(repo, cur, id)? => {
            repo.reference(
                ref_name,
                id,
                PreviousValue::ExistingMustMatch(Target::Peeled(cur)),
                "fast forward",
            )?;
            Ok(true)
        }
        Some(..) => Ok(false),
    }
}

static CELL: OnceCell<Environment> = OnceCell::new();

struct Environment {
    ssh_agent_pid: Option<OsString>,
    ssh_auth_sock: Option<OsString>,
    git_ssh_command: Option<OsString>,
    git_ssh: Option<OsString>,
    git_askpass: Option<OsString>,
}

impl Environment {
    fn new() -> Environment {
        Environment {
            ssh_agent_pid: std::env::var_os("SSH_AGENT_PID"),
            ssh_auth_sock: std::env::var_os("SSH_AUTH_SOCK"),
            git_ssh_command: std::env::var_os("GIT_SSH_COMMAND"),
            git_ssh: std::env::var_os("GIT_SSH"),
            git_askpass: std::env::var_os("GIT_ASKPASS"),
        }
    }

    fn apply(&self, cmd: &mut std::process::Command) {
        Self::maybe_set(cmd, "SSH_AGENT_PID", self.ssh_agent_pid.as_ref());
        Self::maybe_set(cmd, "SSH_AUTH_SOCK", self.ssh_auth_sock.as_ref());
        Self::maybe_set(cmd, "GIT_SSH_COMMAND", self.git_ssh_command.as_ref());
        Self::maybe_set(cmd, "GIT_SSH", self.git_ssh.as_ref());
        Self::maybe_set(cmd, "GIT_ASKPASS", self.git_askpass.as_ref());
    }

    fn maybe_set(cmd: &mut std::process::Command, key: &str, value: Option<&OsString>) {
        if let Some(value) = value {
            cmd.env(key, value);
        }
    }
}

pub fn git_command() -> std::process::Command {
    let environment = CELL.get_or_init(Environment::new);
    let mut cmd = std::process::Command::new("git");
    cmd.env_clear()
        .env("GIT_CONFIG_NOSYSTEM", "")
        .env("GIT_COMMITTER_EMAIL", "you@example.com")
        .env("GIT_COMMITTER_NAME", "Test User")
        .env("GIT_AUTHOR_EMAIL", "you@example.com")
        .env("GIT_AUTHOR_NAME", "Test User");
    environment.apply(&mut cmd);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());
    cmd
}
