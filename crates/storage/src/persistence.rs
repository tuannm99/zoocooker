use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zoocooker_protocol::command::Command;

use crate::store::TreeStore;

const SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("unsupported snapshot version: {0}")]
    UnsupportedSnapshotVersion(u32),
}

#[derive(Debug, Clone)]
pub struct Wal {
    path: PathBuf,
}

impl Wal {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn append(&self, zxid: u64, command: &Command) -> Result<(), PersistenceError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let record = WalRecord {
            zxid,
            command: command.clone(),
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        write_record(&mut file, &record)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<(u64, Command)>, PersistenceError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let mut file = File::open(&self.path)?;
        let mut records = Vec::new();
        loop {
            let mut len_buf = [0_u8; 8];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err.into()),
            }

            let len = u64::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0_u8; len];
            match file.read_exact(&mut payload) {
                Ok(()) => {
                    let record: WalRecord = serde_json::from_slice(&payload)?;
                    records.push((record.zxid, record.command));
                }
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(records)
    }

    /// Rewrites the WAL keeping only records with `zxid > min_zxid`.
    ///
    /// Used after a snapshot is durably written: records already reflected
    /// in the snapshot (zxid <= min_zxid) are dropped, while any record
    /// appended concurrently with the snapshot (zxid > min_zxid) survives.
    /// The rewrite goes through a temp file + rename so a crash mid-compact
    /// never leaves a truncated or corrupt WAL.
    pub fn compact_below(&self, min_zxid: u64) -> Result<(), PersistenceError> {
        let records = self.replay()?;
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut tmp_name = self
            .path
            .file_name()
            .map(|name| name.to_os_string())
            .unwrap_or_default();
        tmp_name.push(".compact.tmp");
        let tmp_path = self.path.with_file_name(tmp_name);

        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            for (zxid, command) in records.into_iter().filter(|(zxid, _)| *zxid > min_zxid) {
                write_record(&mut tmp, &WalRecord { zxid, command })?;
            }
            tmp.sync_all()?;
        }
        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WalRecord {
    zxid: u64,
    command: Command,
}

fn write_record(file: &mut File, record: &WalRecord) -> Result<(), PersistenceError> {
    let payload = serde_json::to_vec(record)?;
    file.write_all(&(payload.len() as u64).to_le_bytes())?;
    file.write_all(&payload)?;
    Ok(())
}

/// Applies WAL records to `store`, skipping any record already reflected in
/// the store (zxid <= the store's current zxid). This makes replay safe to
/// run on top of a snapshot even if the WAL still contains commands the
/// snapshot already captured (e.g. a crash between snapshot write and WAL
/// compaction).
pub fn replay_into(
    store: &mut TreeStore,
    records: Vec<(u64, Command)>,
) -> Result<(), zoocooker_protocol::error::ZkError> {
    for (zxid, command) in records {
        if zxid <= store.current_zxid() {
            continue;
        }
        store.apply(command)?;
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    store: TreeStore,
}

pub fn save_snapshot(path: impl AsRef<Path>, store: &TreeStore) -> Result<(), PersistenceError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let snapshot = Snapshot {
        version: SNAPSHOT_VERSION,
        store: store.clone(),
    };
    let mut file = File::create(path)?;
    serde_json::to_writer(&mut file, &snapshot)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

pub fn load_snapshot(path: impl AsRef<Path>) -> Result<Option<TreeStore>, PersistenceError> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(None);
    }

    let file = File::open(path)?;
    let snapshot: Snapshot = serde_json::from_reader(file)?;
    if snapshot.version != SNAPSHOT_VERSION {
        return Err(PersistenceError::UnsupportedSnapshotVersion(
            snapshot.version,
        ));
    }
    Ok(Some(snapshot.store))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::tempdir;
    use zoocooker_protocol::command::Command;

    use super::{Wal, load_snapshot, replay_into, save_snapshot};
    use crate::store::TreeStore;

    #[test]
    fn wal_appends_and_replays_commands_in_order() {
        let dir = tempdir().unwrap();
        let wal = Wal::new(dir.path().join("commands.wal"));
        let create = Command::Create {
            path: "/app".to_string(),
            data: b"one".to_vec(),
            ephemeral: false,
            sequential: false,
            session_id: None,
        };
        let set = Command::SetData {
            path: "/app".to_string(),
            data: b"two".to_vec(),
            expected_version: Some(0),
        };

        wal.append(1, &create).unwrap();
        wal.append(2, &set).unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 2);
        let mut store = TreeStore::new();
        replay_into(&mut store, records).unwrap();
        assert_eq!(store.get_data("/app").unwrap(), (b"two".to_vec(), 1));
    }

    #[test]
    fn replay_into_skips_records_already_reflected_in_store() {
        let mut store = TreeStore::new();
        store
            .create("/app", b"one".to_vec(), false, false, None)
            .unwrap();
        assert_eq!(store.current_zxid(), 1);

        let stale = Command::Create {
            path: "/app".to_string(),
            data: b"stale".to_vec(),
            ephemeral: false,
            sequential: false,
            session_id: None,
        };
        let fresh = Command::SetData {
            path: "/app".to_string(),
            data: b"fresh".to_vec(),
            expected_version: Some(0),
        };

        replay_into(&mut store, vec![(1, stale), (2, fresh)]).unwrap();
        assert_eq!(store.get_data("/app").unwrap(), (b"fresh".to_vec(), 1));
    }

    #[test]
    fn replay_ignores_trailing_partial_record() {
        let dir = tempdir().unwrap();
        let wal = Wal::new(dir.path().join("commands.wal"));
        wal.append(
            1,
            &Command::Create {
                path: "/app".to_string(),
                data: Vec::new(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            },
        )
        .unwrap();

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(wal.path())
            .unwrap();
        file.write_all(&32_u64.to_le_bytes()).unwrap();
        file.write_all(b"partial").unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn compact_below_drops_snapshotted_records_and_keeps_newer_ones() {
        let dir = tempdir().unwrap();
        let wal = Wal::new(dir.path().join("commands.wal"));
        wal.append(
            1,
            &Command::Create {
                path: "/app".to_string(),
                data: Vec::new(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            },
        )
        .unwrap();
        wal.append(
            2,
            &Command::SetData {
                path: "/app".to_string(),
                data: b"two".to_vec(),
                expected_version: Some(0),
            },
        )
        .unwrap();

        wal.compact_below(1).unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, 2);
    }

    #[test]
    fn snapshot_round_trips_tree_store() {
        let dir = tempdir().unwrap();
        let snapshot_path = dir.path().join("snapshot.json");
        let mut store = TreeStore::new();
        store
            .create("/app", b"one".to_vec(), false, false, None)
            .unwrap();

        save_snapshot(&snapshot_path, &store).unwrap();
        let restored = load_snapshot(&snapshot_path).unwrap().unwrap();
        assert_eq!(restored.get_data("/app").unwrap(), (b"one".to_vec(), 0));
        assert_eq!(restored.child_names("/").unwrap(), vec!["app".to_string()]);
    }
}
