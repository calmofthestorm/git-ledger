use anyhow::{Context, Result};
use gix::object::Kind;
use gix::{Repository, Tree};
use gix_hash::ObjectId;
use gix_object::{
    tree::{self, EntryMode},
    Tree as TreeBuilder,
};
use rand::Rng;
use std::convert::TryInto;
use std::time::{Duration, Instant};

use crate::GitLedger;

/// Degenerate case of `GitLedger` where state is a single blob, permitting a
/// simpler API. Locks with a lease.
#[derive(Clone)]
pub struct BlobGitLedger {
    inner: GitLedger,
    poll_time: Duration,
    lease_length: Duration,
}

pub struct BlobGitLedgerGuard {
    inner: GitLedger,
    commit: Option<ObjectId>,
    lease: u64,
    data: Vec<u8>,
}

impl BlobGitLedger {
    pub fn new(inner: GitLedger, poll_time: Duration, lease_length: Duration) -> BlobGitLedger {
        log::trace!(
            "Initialize BlobGitLedger with poll_time {:?} and lease length {:?}",
            poll_time,
            lease_length
        );
        BlobGitLedger {
            inner,
            poll_time,
            lease_length,
        }
    }

    pub fn lock(&self) -> Result<BlobGitLedgerGuard> {
        loop {
            let mut start_time = Instant::now();
            let mut old_lease = 0;
            log::trace!(
                "Attempt to lock BlobGitLedger start_time={:?} old_lease={:?}",
                start_time,
                old_lease
            );
            let (commit, data) = loop {
                log::trace!("Fetch remote data");
                let (commit, data, lease) = match self.inner.fetch()? {
                    None => {
                        log::trace!("No remote data found; using default.");
                        (None, Vec::default(), 0)
                    }
                    Some((commit, tree)) => {
                        let (data, lease) = decode(&self.inner.repo, tree)?;
                        let commit_id: ObjectId = commit.id.into();
                        log::trace!("Found commit {}", &commit_id);
                        (Some(commit_id), data, lease)
                    }
                };

                if lease == 0 {
                    log::trace!("Existing lease=0; claiming immediately");
                    break (commit, data);
                }

                if lease != old_lease {
                    old_lease = lease;
                    start_time = Instant::now();
                    log::trace!(
                        "old_lease={}; remote lease={}, waiting for expiry starting at {:?}",
                        old_lease,
                        lease,
                        start_time
                    );
                }

                let elapsed = start_time.elapsed();

                if elapsed >= self.lease_length {
                    log::trace!(
                        "Waited long enough for remote lease {} to expire",
                        old_lease
                    );
                    break (commit, data);
                }

                log::trace!(
                    "Sleeping; waiting for lease {}; remaining={:?}",
                    old_lease,
                    (self.lease_length - elapsed)
                );
                std::thread::sleep(std::cmp::min(
                    self.poll_time,
                    self.lease_length
                        .checked_sub(elapsed)
                        .unwrap_or(self.lease_length),
                ));
            };

            let lease: u64 = rand::thread_rng().gen();
            log::trace!("Acquiring with lease={}", lease);
            let tb = encode(&self.inner.repo, &data, lease)?;
            if let Some(commit) = self.inner.push(commit, &tb)? {
                return Ok(BlobGitLedgerGuard {
                    inner: self.inner.clone(),
                    lease,
                    commit: Some(commit),
                    data,
                });
            }
        }
    }
}

impl BlobGitLedgerGuard {
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Update the data and renew the lease.
    pub fn update(&mut self, data: &[u8]) -> Result<()> {
        let old_lease = self.lease;
        self.lease = rand::thread_rng().gen();
        let tb = encode(&self.inner.repo, &data, self.lease)?;
        let commit = self
            .inner
            .push(self.commit, &tb)?
            .with_context(|| format!("Lost lease {}", old_lease))?;
        self.commit = Some(commit);
        self.data.clear();
        self.data.extend_from_slice(data);
        Ok(())
    }

    /// Update the data and release the lease.
    pub fn update_and_release(self, data: &[u8]) -> Result<()> {
        let old_lease = self.lease;
        let tb = encode(&self.inner.repo, &data, 0)?;
        self.inner
            .push(self.commit, &tb)?
            .with_context(|| format!("Lost lease {}", old_lease))?;
        Ok(())
    }

    /// Release the lease. This will give an error if it was lost.
    pub fn release(mut self) -> Result<()> {
        self.release_internal()
    }

    /// Renew the lease.
    pub fn renew(&mut self) -> Result<()> {
        let old_lease = self.lease;
        self.lease = rand::thread_rng().gen();
        let tb = encode(&self.inner.repo, &self.data, self.lease)?;
        let commit = self
            .inner
            .push(self.commit, &tb)?
            .with_context(|| format!("Lost lease {}", old_lease))?;
        self.commit = Some(commit);
        Ok(())
    }

    fn release_internal(&mut self) -> Result<()> {
        let old_lease = self.lease;
        let tb = encode(&self.inner.repo, &self.data, 0)?;
        self.inner
            .push(self.commit, &tb)?
            .with_context(|| format!("Lost lease {}", old_lease))?;
        Ok(())
    }
}

impl Drop for BlobGitLedgerGuard {
    fn drop(&mut self) {
        self.release_internal().ok();
    }
}

fn decode(repo: &Repository, tree: Tree<'_>) -> Result<(Vec<u8>, u64)> {
    let tree = tree.decode()?;
    if tree.entries.len() > 1 {
        anyhow::bail!("unexpected tree entries");
    }
    for entry in tree.entries.iter() {
        let filename: &[u8] = entry.filename.as_ref();
        let filename = hex::decode(filename)?;
        let lease = u64::from_le_bytes(
            filename
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid entry format"))?,
        );
        let blob = repo.find_object(entry.oid)?;
        if blob.kind != Kind::Blob {
            anyhow::bail!("not a blob");
        }
        return Ok((blob.data.to_vec(), lease));
    }
    unreachable!()
}

fn encode(repo: &Repository, data: &[u8], lease: u64) -> Result<TreeBuilder> {
    let blob = repo.write_blob(&data)?;
    let mut tb = TreeBuilder::empty();
    tb.entries.push(tree::Entry {
        oid: blob.into(),
        mode: EntryMode::Blob,
        filename: hex::encode(&lease.to_le_bytes()).into(),
    });
    Ok(tb)
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! setup {
        ($n:expr) => {{
            let tmp = tempdir::TempDir::new("unit.test").unwrap();
            let path = tmp.path();
            let upstream_path = path.join("upstream");
            gix::init_bare(&upstream_path).unwrap();

            let make_ledger = |j|
            {
                let local_path = path.join(format!("local{}", j));
                BlobGitLedger::new(
                    GitLedger::new(
                        local_path.clone(),
                        upstream_path.to_string_lossy().to_string(),
                        "origin".to_string(),
                        "main".to_string(),
                    )
                        .unwrap(),
                    Duration::from_millis(50),
                    Duration::from_millis(500),
                )
            };

            let mut j = 0;
            let ledgers: [BlobGitLedger; $n] = arr_macro::arr![make_ledger({j += 1; j}); $n];

            (tmp, ledgers)
        }};
        () => {{
            let (tmp, ledgers) = setup!(1);
            let ledger = ledgers.into_iter().next().unwrap();
            (tmp, ledger)
        }};
    }

    #[test]
    fn test_blob_ledger() {
        let (_tmp, ledger) = setup!();

        let mut gledger = ledger.lock().unwrap();
        assert_eq!(gledger.data(), b"");
        gledger.update(b"foo").unwrap();
        assert_eq!(gledger.data(), b"foo");
        gledger.update_and_release(b"bar").unwrap();

        let mut gledger = ledger.lock().unwrap();
        assert_eq!(gledger.data(), b"bar");
        gledger.renew().unwrap();
        gledger.update(b"qux").unwrap();
        assert_eq!(gledger.data(), b"qux");
        gledger.renew().unwrap();
        gledger.release().unwrap();

        let gledger = ledger.lock().unwrap();
        assert_eq!(gledger.data(), b"qux");
    }

    #[test]
    fn test_lost_lease() {
        let (_tmp, ledger) = setup!();

        let mut gledger = ledger.lock().unwrap();
        assert_eq!(gledger.data(), b"");
        gledger.update(b"foo").unwrap();
        assert_eq!(gledger.data(), b"foo");

        let mut other = BlobGitLedgerGuard {
            inner: gledger.inner.clone(),
            commit: gledger.commit.clone(),
            data: gledger.data.clone(),
            lease: gledger.lease,
        };
        other.renew().unwrap();

        assert!(gledger.renew().is_err());
        other.renew().unwrap();

        assert_eq!(other.data(), b"foo");
        other.update(b"baz").unwrap();
        assert_eq!(other.data(), b"baz");
    }

    #[test]
    fn test_stolen_lease() {
        let (_tmp, ledger) = setup!();

        let mut gledger = ledger.lock().unwrap();
        gledger.update(b"foo").unwrap();
        std::mem::forget(gledger);

        let start = Instant::now();
        let gledger = ledger.lock().unwrap();
        assert!(start.elapsed() >= Duration::from_millis(500));
        assert_eq!(gledger.data(), b"foo");
    }

    #[test]
    fn test_lease() {
        let (_tmp, ledgers) = setup!(2);
        let mut ledgers = ledgers.into_iter();

        let ledger1 = ledgers.next().unwrap();
        let ledger2 = ledgers.next().unwrap();

        let t1 = std::thread::spawn(move || {
            for j in 0..4 {
                loop {
                    let guard = ledger1.lock().unwrap();
                    if guard.data().len() == 2 * j {
                        let mut data = guard.data().to_vec();
                        data.push(b' ');
                        guard.update_and_release(&data).unwrap();
                        break;
                    }
                    guard.release().unwrap();
                    std::thread::sleep(Duration::from_millis(500));
                }
            }
        });
        let t2 = std::thread::spawn(move || {
            for j in 0..4 {
                loop {
                    let guard = ledger2.lock().unwrap();
                    if guard.data().len() == 2 * j + 1 {
                        let mut data = guard.data().to_vec();
                        data.push(b' ');
                        guard.update_and_release(&data).unwrap();
                        break;
                    }
                    guard.release().unwrap();
                    std::thread::sleep(Duration::from_millis(500));
                }
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }
}
