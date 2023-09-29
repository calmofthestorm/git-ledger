use std::path::PathBuf;

use anyhow::{Context, Result};
use gix::object::Kind;
use gix::progress::Discard as DiscardProgress;
use gix::remote::Direction;
use gix::{Commit, Repository};
use gix_hash::ObjectId;
use gix_object::Tree as TreeBuilder;
use rand::Rng;

use crate::util::*;

/// Manages a monotonic ledger stored as a root tree on a branch in a local git
/// repository, and synchronizes to upstream. Ledgers may be updated using the
/// `update_[once_]with` API, which takes a function (idempotent if not once
/// version) and repeatedly fetches the upstream state, applies the function,
/// then attempts to push a commit containing the new tree, or the more general
/// API provided by fetch / push.
#[derive(Clone, Debug)]
pub struct GitLedger {
    pub repo: Repository,
    local_path: PathBuf,
    branch_ref: String,
    tracking_ref: String,
    remote_name: String,
    tmp_ref: String,
}

impl GitLedger {
    pub fn new(
        local_path: PathBuf,
        remote_spec: String,
        remote_name: String,
        branch_name: String,
    ) -> Result<GitLedger> {
        let mut repo = init_repo(&local_path, &remote_spec, &remote_name, true)?;
        repo.object_cache_size_if_unset(4 * 1024 * 1024);
        let tmp_ref = format!("refs/tmp/tmp{}", rand::thread_rng().gen::<u64>());
        let branch_ref = format!("refs/heads/{}", &branch_name);
        let tracking_ref = format!("remotes/{}/{}", &remote_name, &branch_name);
        Ok(GitLedger {
            repo,
            local_path,
            remote_name,
            branch_ref,
            tracking_ref,
            tmp_ref,
        })
    }

    pub fn update_once_with<'r, F, E>(&self, f: F) -> Result<Option<()>>
    where
        E: Into<anyhow::Error> + std::marker::Send + std::marker::Sync + 'static,
        F: FnOnce(
            &Repository,
            Option<(Commit<'_>, gix::Tree<'_>)>,
        ) -> std::result::Result<TreeBuilder, E>,
    {
        let old = self.fetch()?;
        let root_commit = match old.as_ref() {
            Some((root_commit, _)) => Some(root_commit.id()),
            None => None,
        };
        let tree = f(&self.repo, old).map_err(Into::into)?;
        Ok(self.push(root_commit.map(Into::into), &tree)?.map(|_| ()))
    }

    pub fn update_with<'r, F, E>(&self, mut f: F) -> Result<()>
    where
        E: Into<anyhow::Error> + std::marker::Send + std::marker::Sync + 'static,
        F: FnMut(
            &Repository,
            Option<(Commit<'_>, gix::Tree<'_>)>,
        ) -> std::result::Result<TreeBuilder, E>,
    {
        loop {
            if let Some(tb) = self.update_once_with(&mut f)? {
                return Ok(tb);
            }
        }
    }

    pub fn fetch(&self) -> Result<Option<(Commit<'_>, gix::Tree<'_>)>> {
        self.fetch_refs()?;

        let reference = match self.repo.try_find_reference(&self.branch_ref)? {
            Some(r) => r,
            None => return Ok(None),
        };
        let root_id = reference.clone().into_fully_peeled_id()?;

        let root_commit = self.repo.try_find_object(root_id)?.context("root commit")?;
        if root_commit.kind != Kind::Commit {
            anyhow::bail!("Expected commit");
        }
        let root_commit = root_commit.into_commit();
        let root_tree = root_commit.tree()?;

        Ok(Some((root_commit, root_tree)))
    }

    pub fn push(
        &self,
        old_commit_id: Option<ObjectId>,
        tree: &TreeBuilder,
    ) -> Result<Option<ObjectId>> {
        let tree = self.repo.write_object(&tree).context("write tree to git")?;

        // FIXME: There is a brief race window here that would see tmp not cleaned
        // up.
        let new_commit_id: ObjectId = self
            .repo
            .commit(
                self.tmp_ref.as_str(),
                "A Commit In Time",
                tree,
                old_commit_id.into_iter(),
            )
            .context("commit to git")?
            .into();

        let result = match git_command()
            .current_dir(&self.local_path)
            .arg("push")
            .arg(&self.remote_name)
            .arg(format!("{}:{}", &self.tmp_ref, self.branch_ref))
            .status()
        {
            Ok(status) if status.success() => Ok(Some(new_commit_id)),
            Ok(..) => match self.maybe_raced(old_commit_id) {
                Ok(true) => Ok(None),
                Ok(false) => anyhow::bail!("a git command failed"),
                Err(e) => Err(e).context("maybe raced"),
            },
            Err(e) => Err(e).context("subprocess failed"),
        };

        self.repo
            .find_reference(self.tmp_ref.as_str())
            .context("find_reference")?
            .delete()
            .context("delete")?;

        result
    }

    fn fetch_refs(&self) -> Result<()> {
        let interrupted = core::sync::atomic::AtomicBool::new(false);
        let remote = self.repo.find_remote(self.remote_name.as_str())?;
        let remote = remote.connect(Direction::Fetch)?;
        let fetch =
            remote.prepare_fetch(DiscardProgress, gix::remote::ref_map::Options::default())?;
        fetch.receive(DiscardProgress, &interrupted)?;
        if interrupted.load(core::sync::atomic::Ordering::SeqCst) {
            anyhow::bail!("Interrupted.");
        }

        if !fast_forward_reference(&self.repo, &self.branch_ref, &self.tracking_ref)? {
            anyhow::bail!("Tracking branch cannot fast forward.");
        }

        Ok(())
    }

    fn maybe_raced(&self, old_commit_id: Option<ObjectId>) -> Result<bool> {
        self.fetch_refs()?;
        let remote_id = peeled_only(self.repo.refs.try_find(&self.tracking_ref)?)?;

        if old_commit_id != remote_id {
            log::trace!("maybe_raced: {:?} != {:?}", &old_commit_id, &remote_id);
            // TODO: Structured errors for this crate. In particular, the option
            // returns are dangerous because there's no warning if they are
            // ignored.
            return Ok(true);
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use gix_object::tree::{Entry, EntryMode};

    macro_rules! init {
        ($n:expr, $path:expr) => {{
            let upstream_path = $path.join("upstream");
            if !upstream_path.exists() {
                gix::init_bare(&upstream_path).unwrap();
            }

            let make_ledger = |j|
            {
                let local_path = $path.join(format!("local{}", j));
                GitLedger::new(
                    local_path.clone(),
                    upstream_path.to_string_lossy().to_string(),
                    "origin".to_string(),
                    "main".to_string(),
                )
                    .unwrap()
            };

            let mut j = 0;
            let ledgers: [GitLedger; $n] = arr_macro::arr![make_ledger({j += 1; j}); $n];
            ledgers
        }};

        ($path:expr) => {{
            init!(1, $path).into_iter().next().unwrap()
        }};
    }

    #[test]
    fn test_update_with() {
        let tmp = tempdir::TempDir::new("unit.test").unwrap();
        let mut gledger = init!(tmp.path());
        for i in 0..10 {
            gledger
                .update_with(|repo, st| match st {
                    None => {
                        let a = repo.write_blob(b"0")?;
                        let mut tb = TreeBuilder::empty();
                        tb.entries.push(Entry {
                            oid: a.into(),
                            mode: EntryMode::Blob,
                            filename: "single".into(),
                        });
                        let r: Result<_> = Ok(tb);
                        r
                    }
                    Some((_commit, tree)) => {
                        let a = tree.lookup_entry_by_path("single").unwrap().unwrap();
                        let a = repo.find_object(a.oid()).unwrap();
                        assert_eq!(a.kind, Kind::Blob);
                        let a: u64 = std::str::from_utf8(&a.data).unwrap().parse().unwrap();

                        assert_eq!(a, (i - 1));

                        let a = repo.write_blob((a + 1).to_string())?;

                        let mut tb = TreeBuilder::empty();
                        tb.entries.push(Entry {
                            oid: a.into(),
                            mode: EntryMode::Blob,
                            filename: "single".into(),
                        });
                        let r: Result<_> = Ok(tb);
                        r
                    }
                })
                .unwrap();

            if i == 5 {
                // Removing local data has no effect as it is persisted upstream.
                std::fs::remove_dir_all(&gledger.local_path).unwrap();
                gledger = init!(tmp.path());
            }
        }
    }

    #[test]
    fn test_conflict() {
        let tmp = tempdir::TempDir::new("unit.test").unwrap();
        let mut gledgers = init!(2, tmp.path()).into_iter();

        let gledger1 = gledgers.next().unwrap();
        let gledger2 = gledgers.next().unwrap();

        let a = gledger1.repo.write_blob(b"0").unwrap();
        let mut tb1 = TreeBuilder::empty();
        tb1.entries.push(Entry {
            oid: a.into(),
            mode: EntryMode::Blob,
            filename: "single".into(),
        });

        let b = gledger2.repo.write_blob(b"27").unwrap();
        let mut tb2 = TreeBuilder::empty();
        tb2.entries.push(Entry {
            oid: b.into(),
            mode: EntryMode::Blob,
            filename: "double".into(),
        });

        gledger1.push(None, &tb1).unwrap().unwrap();
        assert!(gledger2.push(None, &tb2).unwrap().is_none());

        let (commit1, tree1) = gledger2.fetch().unwrap().unwrap();
        gledger2
            .push(Some(commit1.id().into()), &tb2)
            .unwrap()
            .unwrap();

        let (_commit2, tree2) = gledger1.fetch().unwrap().unwrap();

        assert_eq!(
            tree1.lookup_entry_by_path("single").unwrap().unwrap().oid(),
            ObjectId::from(a)
        );
        assert_eq!(
            tree2.lookup_entry_by_path("double").unwrap().unwrap().oid(),
            ObjectId::from(b)
        );
    }
}
