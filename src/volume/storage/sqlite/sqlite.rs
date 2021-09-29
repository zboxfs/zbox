use std::ffi::{CStr, CString};
use std::fmt::{self, Debug};
use std::os::raw::{c_int, c_void};
use std::ptr;
use std::thread::panicking;

use libsqlite3_sys as ffi;
use log::warn;

use crate::base::crypto::{Crypto, Key};
use crate::base::vio;
use crate::error::{Error, Result};
use crate::trans::Eid;
use crate::volume::address::Span;
use crate::volume::storage::Storable;
use crate::volume::BLK_SIZE;

// check result code returned by sqlite
fn check_result(result: c_int) -> Result<()> {
    if result != ffi::SQLITE_OK {
        let err = ffi::Error::new(result);
        return Err(Error::from(err));
    }
    Ok(())
}

// reset and clean up statement
fn reset_stmt(stmt: *mut ffi::sqlite3_stmt) -> Result<()> {
    let result = unsafe { ffi::sqlite3_reset(stmt) };
    check_result(result)?;
    let result = unsafe { ffi::sqlite3_clear_bindings(stmt) };
    check_result(result)?;
    Ok(())
}

// bind integer parameter
fn bind_int(
    stmt: *mut ffi::sqlite3_stmt,
    col_idx: c_int,
    n: usize,
) -> Result<()> {
    let result = unsafe { ffi::sqlite3_bind_int(stmt, col_idx, n as c_int) };
    check_result(result)
}

// bind EID parameter
fn bind_id(
    stmt: *mut ffi::sqlite3_stmt,
    col_idx: c_int,
    id_str: &CStr,
) -> Result<()> {
    let result = unsafe {
        ffi::sqlite3_bind_text(
            stmt,
            col_idx,
            id_str.as_ptr(),
            -1,
            ffi::SQLITE_STATIC(),
        )
    };
    check_result(result)
}

// bind blob parameter
fn bind_blob(
    stmt: *mut ffi::sqlite3_stmt,
    col_idx: c_int,
    data: &[u8],
) -> Result<()> {
    let result = unsafe {
        ffi::sqlite3_bind_blob(
            stmt,
            col_idx,
            data.as_ptr() as *const c_void,
            data.len() as c_int,
            ffi::SQLITE_STATIC(),
        )
    };
    check_result(result)
}

// run DML statement, such as INSERT and DELETE
fn run_dml(stmt: *mut ffi::sqlite3_stmt) -> Result<()> {
    let result = unsafe { ffi::sqlite3_step(stmt) };
    match result {
        ffi::SQLITE_DONE => Ok(()),
        _ => Err(Error::from(ffi::Error::new(result))),
    }
}

// run SELECT statement on a blob column
fn run_select_blob(stmt: *mut ffi::sqlite3_stmt) -> Result<Vec<u8>> {
    let result = unsafe { ffi::sqlite3_step(stmt) };
    match result {
        ffi::SQLITE_ROW => {
            //  get data and data size
            let (data, data_len) = unsafe {
                (
                    ffi::sqlite3_column_blob(stmt, 0),
                    ffi::sqlite3_column_bytes(stmt, 0) as usize,
                )
            };

            // copy data to vec and return it
            let mut ret = vec![0u8; data_len];
            unsafe {
                ptr::copy_nonoverlapping(
                    data,
                    (&mut ret).as_mut_ptr() as *mut c_void,
                    data_len,
                );
            }
            Ok(ret)
        }
        ffi::SQLITE_DONE => Err(Error::NotFound),
        _ => Err(Error::from(ffi::Error::new(result))),
    }
}

/// Sqlite Storage
pub struct SqliteStorage {
    is_attached: bool,  // attached to sqlite db
    file_path: CString, // database file path
    db: *mut ffi::sqlite3,
    stmts: Vec<*mut ffi::sqlite3_stmt>,
}

impl SqliteStorage {
    // table name constants
    const TBL_REPO_LOCK: &'static str = "repo_lock";
    const TBL_SUPER_BLOCK: &'static str = "super_block";
    const TBL_WALS: &'static str = "wals";
    const TBL_ADDRESSES: &'static str = "addresses";
    const TBL_BLOCKS: &'static str = "blocks";

    pub fn new(file_path: &str) -> Self {
        SqliteStorage {
            is_attached: false,
            file_path: CString::new(file_path).unwrap(),
            db: ptr::null_mut(),
            stmts: Vec::with_capacity(14),
        }
    }

    // prepare one sql statement
    fn prepare_sql(&mut self, sql: String) -> Result<()> {
        let mut stmt = ptr::null_mut();
        let sql = CString::new(sql).unwrap();
        let result = unsafe {
            ffi::sqlite3_prepare_v2(
                self.db,
                sql.as_ptr(),
                -1,
                &mut stmt,
                ptr::null_mut(),
            )
        };
        check_result(result)?;
        self.stmts.push(stmt);
        Ok(())
    }

    // prepare and cache all sql statements
    fn prepare_stmts(&mut self) -> Result<()> {
        // check if all statements are prepared
        if self.stmts.len() == 14 {
            return Ok(());
        }

        self.stmts.clear();

        // repo lock sql
        self.prepare_sql(format!(
            "
            SELECT lock FROM {} WHERE lock = 1
        ",
            Self::TBL_REPO_LOCK
        ))?;
        self.prepare_sql(format!(
            "
            INSERT OR REPLACE INTO {}(lock) VALUES (1)
        ",
            Self::TBL_REPO_LOCK
        ))?;
        self.prepare_sql(format!(
            "
            DELETE FROM {} WHERE lock = 1
        ",
            Self::TBL_REPO_LOCK
        ))?;

        // super block sql
        self.prepare_sql(format!(
            "
            SELECT data FROM {} WHERE suffix = ?
        ",
            Self::TBL_SUPER_BLOCK
        ))?;
        self.prepare_sql(format!(
            "
            INSERT OR REPLACE INTO {}(suffix, data) VALUES (?, ?)
        ",
            Self::TBL_SUPER_BLOCK
        ))?;

        // wal sql
        self.prepare_sql(format!(
            "
            SELECT data FROM {} WHERE id = ?
        ",
            Self::TBL_WALS
        ))?;
        self.prepare_sql(format!(
            "
            INSERT OR REPLACE INTO {}(id, data) VALUES (?, ?)
        ",
            Self::TBL_WALS
        ))?;
        self.prepare_sql(format!(
            "
            DELETE FROM {} WHERE id = ?
        ",
            Self::TBL_WALS
        ))?;

        // addresses sql
        self.prepare_sql(format!(
            "
            SELECT data FROM {} WHERE id = ?
        ",
            Self::TBL_ADDRESSES
        ))?;
        self.prepare_sql(format!(
            "
            INSERT OR REPLACE INTO {}(id, data) VALUES (?, ?)
        ",
            Self::TBL_ADDRESSES
        ))?;
        self.prepare_sql(format!(
            "
            DELETE FROM {} WHERE id = ?
        ",
            Self::TBL_ADDRESSES
        ))?;

        // blocks sql
        self.prepare_sql(format!(
            "
            SELECT data FROM {} WHERE blk_idx = ?
        ",
            Self::TBL_BLOCKS
        ))?;
        self.prepare_sql(format!(
            "
            INSERT INTO {}(blk_idx, data) VALUES (?, ?)
        ",
            Self::TBL_BLOCKS
        ))?;
        self.prepare_sql(format!(
            "
            DELETE FROM {} WHERE blk_idx = ?
        ",
            Self::TBL_BLOCKS
        ))?;

        Ok(())
    }

    fn lock_repo(&mut self, force: bool) -> Result<()> {
        let stmt = self.stmts[0];
        reset_stmt(stmt)?;
        let result = unsafe { ffi::sqlite3_step(stmt) };
        match result {
            ffi::SQLITE_ROW => {
                // repo is locked
                if force {
                    warn!("Repo was locked, forced to open");
                    self.is_attached = true;
                    Ok(())
                } else {
                    Err(Error::RepoOpened)
                }
            }
            ffi::SQLITE_DONE => {
                // repo is not locked yet, lock it now
                let stmt = self.stmts[1];
                reset_stmt(stmt)?;
                run_dml(stmt)?;
                self.is_attached = true;
                Ok(())
            }
            _ => Err(Error::from(ffi::Error::new(result))),
        }
    }
}

impl Storable for SqliteStorage {
    fn exists(&self) -> Result<bool> {
        let mut db: *mut ffi::sqlite3 = ptr::null_mut();
        let result = unsafe {
            ffi::sqlite3_open_v2(
                self.file_path.as_ptr(),
                &mut db,
                ffi::SQLITE_OPEN_READONLY,
                ptr::null(),
            )
        };
        if !db.is_null() {
            unsafe { ffi::sqlite3_close(db) };
        }
        Ok(result == ffi::SQLITE_OK)
    }

    fn connect(&mut self, _force: bool) -> Result<()> {
        let result = unsafe {
            ffi::sqlite3_open_v2(
                self.file_path.as_ptr(),
                &mut self.db,
                ffi::SQLITE_OPEN_READWRITE
                    | ffi::SQLITE_OPEN_CREATE
                    | ffi::SQLITE_OPEN_FULLMUTEX,
                ptr::null(),
            )
        };
        if result != ffi::SQLITE_OK {
            let err = ffi::Error::new(result);
            if !self.db.is_null() {
                unsafe { ffi::sqlite3_close(self.db) };
                self.db = ptr::null_mut();
            }
            return Err(Error::from(err));
        }

        Ok(())
    }

    fn init(&mut self, _crypto: Crypto, _key: Key) -> Result<()> {
        // create tables
        let sql = format!(
            "
            CREATE TABLE {} (
                lock        INTEGER
            );
            CREATE TABLE {} (
                suffix      INTEGER PRIMARY KEY,
                data        BLOB
            );
            CREATE TABLE {} (
                id          TEXT PRIMARY KEY,
                data        BLOB
            );
            CREATE TABLE {} (
                id          TEXT PRIMARY KEY,
                data        BLOB
            );
            CREATE TABLE {} (
                blk_idx     INTEGER PRIMARY KEY,
                data        BLOB
            );
        ",
            Self::TBL_REPO_LOCK,
            Self::TBL_SUPER_BLOCK,
            Self::TBL_WALS,
            Self::TBL_ADDRESSES,
            Self::TBL_BLOCKS
        );
        let sql = CString::new(sql).unwrap();
        let result = unsafe {
            ffi::sqlite3_exec(
                self.db,
                sql.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        check_result(result)?;

        self.prepare_stmts()?;
        self.lock_repo(false)
    }

    #[inline]
    fn open(&mut self, _crypto: Crypto, _key: Key, force: bool) -> Result<()> {
        self.prepare_stmts()?;
        self.lock_repo(force)
    }

    fn get_super_block(&mut self, suffix: u64) -> Result<Vec<u8>> {
        // prepare statements
        self.prepare_stmts()?;

        let stmt = self.stmts[3];
        reset_stmt(stmt)?;

        // bind parameters and run sql
        bind_int(stmt, 1, suffix as usize)?;
        run_select_blob(stmt)
    }

    fn put_super_block(&mut self, super_blk: &[u8], suffix: u64) -> Result<()> {
        let stmt = self.stmts[4];
        reset_stmt(stmt)?;

        // bind parameters and run sql
        bind_int(stmt, 1, suffix as usize)?;
        bind_blob(stmt, 2, super_blk)?;
        run_dml(stmt)
    }

    fn get_wal(&mut self, id: &Eid) -> Result<Vec<u8>> {
        let stmt = self.stmts[5];
        reset_stmt(stmt)?;

        // bind parameters and run sql
        let id_str = CString::new(id.to_string()).unwrap();
        bind_id(stmt, 1, &id_str)?;
        run_select_blob(stmt)
    }

    fn put_wal(&mut self, id: &Eid, wal: &[u8]) -> Result<()> {
        let stmt = self.stmts[6];
        reset_stmt(stmt)?;

        // bind parameters and run sql
        let id_str = CString::new(id.to_string()).unwrap();
        bind_id(stmt, 1, &id_str)?;
        bind_blob(stmt, 2, wal)?;
        run_dml(stmt)
    }

    fn del_wal(&mut self, id: &Eid) -> Result<()> {
        let stmt = self.stmts[7];
        reset_stmt(stmt)?;

        // bind parameters and run sql
        let id_str = CString::new(id.to_string()).unwrap();
        bind_id(stmt, 1, &id_str)?;
        run_dml(stmt)
    }

    fn get_address(&mut self, id: &Eid) -> Result<Vec<u8>> {
        let stmt = self.stmts[8];
        reset_stmt(stmt)?;

        // bind parameters and run sql
        let id_str = CString::new(id.to_string()).unwrap();
        bind_id(stmt, 1, &id_str)?;
        run_select_blob(stmt)
    }

    fn put_address(&mut self, id: &Eid, addr: &[u8]) -> Result<()> {
        let stmt = self.stmts[9];
        reset_stmt(stmt)?;

        // bind parameters and run sql
        let id_str = CString::new(id.to_string()).unwrap();
        bind_id(stmt, 1, &id_str)?;
        bind_blob(stmt, 2, addr)?;
        run_dml(stmt)
    }

    fn del_address(&mut self, id: &Eid) -> Result<()> {
        let stmt = self.stmts[10];
        reset_stmt(stmt)?;

        // bind parameters and run sql
        let id_str = CString::new(id.to_string()).unwrap();
        bind_id(stmt, 1, &id_str)?;
        run_dml(stmt)
    }

    fn get_blocks(&mut self, dst: &mut [u8], span: Span) -> Result<()> {
        let stmt = self.stmts[11];

        let mut read = 0;
        for blk_idx in span {
            // reset statement and binding
            reset_stmt(stmt)?;

            // bind parameters and run sql
            bind_int(stmt, 1, blk_idx)?;
            let blk = run_select_blob(stmt)?;
            assert_eq!(blk.len(), BLK_SIZE);
            dst[read..read + BLK_SIZE].copy_from_slice(&blk);
            read += BLK_SIZE;
        }

        Ok(())
    }

    fn put_blocks(&mut self, span: Span, mut blks: &[u8]) -> Result<()> {
        let stmt = self.stmts[12];

        for blk_idx in span {
            // reset statement and binding
            reset_stmt(stmt)?;

            // bind parameters and run sql
            bind_int(stmt, 1, blk_idx)?;
            bind_blob(stmt, 2, &blks[..BLK_SIZE])?;
            run_dml(stmt)?;

            blks = &blks[BLK_SIZE..];
        }

        Ok(())
    }

    fn del_blocks(&mut self, span: Span) -> Result<()> {
        let stmt = self.stmts[13];

        for blk_idx in span {
            // reset statement and binding
            reset_stmt(stmt)?;

            // bind parameters and run sql
            bind_int(stmt, 1, blk_idx)?;
            run_dml(stmt)?;
        }

        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn destroy(&mut self) -> Result<()> {
        self.connect(false)?;
        if self.prepare_stmts().is_ok() {
            let stmt = self.stmts[0];
            reset_stmt(stmt)?;
            if let ffi::SQLITE_ROW = unsafe { ffi::sqlite3_step(stmt) } {
                // repo is locked
                warn!("Destroy an opened repo");
            }
        }
        vio::remove_file(self.file_path.to_str().unwrap())?;
        Ok(())
    }
}

impl Drop for SqliteStorage {
    fn drop(&mut self) {
        // release repo lock and ignore the result
        if self.is_attached {
            let stmt = self.stmts[2];
            let _ = reset_stmt(stmt).and_then(|_| unsafe {
                ffi::sqlite3_step(stmt);
                Ok(())
            });
            self.is_attached = false;
        }

        // release statements
        unsafe {
            for stmt in self.stmts.iter() {
                ffi::sqlite3_finalize(*stmt);
            }
        }

        // close db connection
        let result = unsafe { ffi::sqlite3_close(self.db) };
        if result != ffi::SQLITE_OK {
            if panicking() {
                eprintln!("Error while closing SQLite connection: {}", result);
            } else {
                panic!("Error while closing SQLite connection: {}", result);
            }
        }
    }
}

impl Debug for SqliteStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SqliteStorage")
            .field("file_path", &self.file_path)
            .finish()
    }
}

unsafe impl Send for SqliteStorage {}
unsafe impl Sync for SqliteStorage {}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use self::tempdir::TempDir;

    use super::*;

    use base::init_env;

    #[test]
    fn sqlite_storage() {
        init_env();
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let dir = tmpdir.path().join("storage.db");
        let mut ss = SqliteStorage::new(dir.to_str().unwrap());

        ss.connect(false).unwrap();
        ss.init(Crypto::default(), Key::new_empty()).unwrap();

        let id = Eid::new();
        let buf = vec![1, 2, 3];
        let blks = vec![42u8; BLK_SIZE * 3];
        let mut dst = vec![0u8; BLK_SIZE * 3];

        // super block
        ss.put_super_block(&buf, 0).unwrap();
        let s = ss.get_super_block(0).unwrap();
        assert_eq!(&s[..], &buf[..]);

        // wal
        ss.put_wal(&id, &buf).unwrap();
        let s = ss.get_wal(&id).unwrap();
        assert_eq!(&s[..], &buf[..]);
        ss.del_wal(&id).unwrap();
        assert_eq!(ss.get_wal(&id).unwrap_err(), Error::NotFound);

        // address
        ss.put_address(&id, &buf).unwrap();
        let s = ss.get_address(&id).unwrap();
        assert_eq!(&s[..], &buf[..]);
        ss.del_address(&id).unwrap();
        assert_eq!(ss.get_address(&id).unwrap_err(), Error::NotFound);

        // block
        let span = Span::new(0, 3);
        ss.put_blocks(span, &blks).unwrap();
        ss.get_blocks(&mut dst, span).unwrap();
        assert_eq!(&dst[..], &blks[..]);
        ss.del_blocks(Span::new(1, 2)).unwrap();
        assert_eq!(ss.get_blocks(&mut dst, span).unwrap_err(), Error::NotFound);
        assert_eq!(
            ss.get_blocks(&mut dst[..BLK_SIZE], Span::new(1, 1))
                .unwrap_err(),
            Error::NotFound
        );
        assert_eq!(
            ss.get_blocks(&mut dst[..BLK_SIZE], Span::new(2, 1))
                .unwrap_err(),
            Error::NotFound
        );

        // re-open
        drop(ss);
        let mut ss = SqliteStorage::new(dir.to_str().unwrap());
        ss.connect(false).unwrap();
        ss.open(Crypto::default(), Key::new_empty(), false).unwrap();

        ss.get_blocks(&mut dst[..BLK_SIZE], Span::new(0, 1))
            .unwrap();
        assert_eq!(&dst[..BLK_SIZE], &blks[..BLK_SIZE]);
        assert_eq!(
            ss.get_blocks(&mut dst[..BLK_SIZE], Span::new(1, 1))
                .unwrap_err(),
            Error::NotFound
        );
        assert_eq!(
            ss.get_blocks(&mut dst[..BLK_SIZE], Span::new(2, 1))
                .unwrap_err(),
            Error::NotFound
        );
    }
}
