use std::env::VarError;
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};
use std::io::Error as IoError;
use std::result;

use rmp_serde::decode::Error as DecodeError;
use rmp_serde::encode::Error as EncodeError;

#[cfg(feature = "storage-sqlite")]
use libsqlite3_sys::Error as SqliteError;

#[cfg(feature = "storage-redis")]
use redis::RedisError;

#[cfg(feature = "storage-zbox")]
use http::{Error as HttpError, StatusCode};

#[cfg(feature = "storage-zbox")]
use serde_json::Error as JsonError;

#[cfg(feature = "storage-zbox-native")]
use reqwest::Error as ReqwestError;

#[cfg(feature = "storage-zbox-android")]
use jni::errors::Error as JniError;

/// The error type for operations with [`Repo`] and [`File`].
///
/// [`Repo`]: struct.Repo.html
/// [`File`]: struct.File.html
#[derive(Debug)]
pub enum Error {
    RefOverflow,
    RefUnderflow,

    InitCrypto,
    NoAesHardware,
    Hashing,
    InvalidCost,
    InvalidCipher,
    Encrypt,
    Decrypt,

    InvalidUri,
    InvalidSuperBlk,
    Corrupted,
    WrongVersion,
    NoEntity,
    NotInSync,
    RepoOpened,
    RepoClosed,
    RepoExists,

    InTrans,
    NotInTrans,
    NoTrans,
    Uncompleted,
    InUse,

    NoContent,

    InvalidArgument,
    InvalidPath,
    NotFound,
    AlreadyExists,
    IsRoot,
    IsDir,
    IsFile,
    NotDir,
    NotFile,
    NotEmpty,
    NoVersion,

    ReadOnly,
    CannotRead,
    CannotWrite,
    NotWrite,
    NotFinish,
    Closed,

    Encode(EncodeError),
    Decode(DecodeError),
    Var(VarError),
    Io(IoError),

    #[cfg(feature = "storage-sqlite")]
    Sqlite(SqliteError),

    #[cfg(feature = "storage-redis")]
    Redis(RedisError),

    #[cfg(feature = "storage-zbox")]
    Http(HttpError),
    #[cfg(feature = "storage-zbox")]
    HttpStatus(StatusCode),
    #[cfg(feature = "storage-zbox")]
    Json(JsonError),

    #[cfg(feature = "storage-zbox-native")]
    Reqwest(ReqwestError),

    #[cfg(feature = "storage-zbox-android")]
    Jni(JniError),

    #[cfg(target_arch = "wasm32")]
    RequestError,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Error::RefOverflow => write!(f, "Refcnt overflow"),
            Error::RefUnderflow => write!(f, "Refcnt underflow"),

            Error::InitCrypto => write!(f, "InitCrypto crypto error"),
            Error::NoAesHardware => write!(f, "No AES hardware"),
            Error::Hashing => write!(f, "Hashing out of memory"),
            Error::InvalidCost => write!(f, "Invalid cost"),
            Error::InvalidCipher => write!(f, "Invalid cipher"),
            Error::Encrypt => write!(f, "Encrypt error"),
            Error::Decrypt => write!(f, "Decrypt error"),

            Error::InvalidUri => write!(f, "Invalid Uri"),
            Error::InvalidSuperBlk => write!(f, "Invalid super block"),
            Error::Corrupted => write!(f, "Volume is corrupted"),
            Error::WrongVersion => write!(f, "Version not match"),
            Error::NoEntity => write!(f, "Entity not found"),
            Error::NotInSync => write!(f, "Repo is not in sync"),
            Error::RepoOpened => write!(f, "Repo is opened"),
            Error::RepoClosed => write!(f, "Repo is closed"),
            Error::RepoExists => write!(f, "Repo already exists"),

            Error::InTrans => write!(f, "Already in transaction"),
            Error::NotInTrans => write!(f, "Not in transaction"),
            Error::NoTrans => write!(f, "Transaction not found"),
            Error::Uncompleted => write!(f, "Transaction uncompleted"),
            Error::InUse => write!(f, "Entity is in use"),

            Error::NoContent => write!(f, "Content not found"),

            Error::InvalidArgument => write!(f, "Invalid argument"),
            Error::InvalidPath => write!(f, "Invalid path"),
            Error::NotFound => write!(f, "File not found"),
            Error::AlreadyExists => write!(f, "File already exists"),
            Error::IsRoot => write!(f, "File is root"),
            Error::IsDir => write!(f, "Path is dir"),
            Error::IsFile => write!(f, "Path is file"),
            Error::NotDir => write!(f, "Path is not dir"),
            Error::NotFile => write!(f, "Path is not file"),
            Error::NotEmpty => write!(f, "Directory is not empty"),
            Error::NoVersion => write!(f, "File has no version"),

            Error::ReadOnly => write!(f, "Opened as read only"),
            Error::CannotRead => write!(f, "Cannot read file"),
            Error::CannotWrite => write!(f, "Cannot write file"),
            Error::NotWrite => write!(f, "File does not write yet"),
            Error::NotFinish => write!(f, "File does not finish yet"),
            Error::Closed => write!(f, "File is closed"),

            Error::Encode(ref err) => err.fmt(f),
            Error::Decode(ref err) => err.fmt(f),
            Error::Var(ref err) => err.fmt(f),
            Error::Io(ref err) => err.fmt(f),

            #[cfg(feature = "storage-sqlite")]
            Error::Sqlite(ref err) => err.fmt(f),

            #[cfg(feature = "storage-redis")]
            Error::Redis(ref err) => err.fmt(f),

            #[cfg(feature = "storage-zbox")]
            Error::Http(ref err) => err.fmt(f),
            #[cfg(feature = "storage-zbox")]
            Error::HttpStatus(status_code) => {
                write!(f, "Http status {}", status_code)
            }
            #[cfg(feature = "storage-zbox")]
            Error::Json(ref err) => err.fmt(f),

            #[cfg(feature = "storage-zbox-native")]
            Error::Reqwest(ref err) => err.fmt(f),

            #[cfg(feature = "storage-zbox-android")]
            Error::Jni(ref err) => err.fmt(f),

            #[cfg(target_arch = "wasm32")]
            Error::RequestError => write!(f, "Http request failed"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match *self {
            Error::Encode(ref err) => Some(err),
            Error::Decode(ref err) => Some(err),
            Error::Var(ref err) => Some(err),
            Error::Io(ref err) => Some(err),

            #[cfg(feature = "storage-sqlite")]
            Error::Sqlite(ref err) => Some(err),

            #[cfg(feature = "storage-redis")]
            Error::Redis(ref err) => Some(err),

            #[cfg(feature = "storage-zbox")]
            Error::Http(ref err) => Some(err),
            #[cfg(feature = "storage-zbox")]
            Error::Json(ref err) => Some(err),

            #[cfg(feature = "storage-zbox-native")]
            Error::Reqwest(ref err) => Some(err),

            #[cfg(feature = "storage-zbox-android")]
            Error::Jni(ref err) => Some(err),

            _ => None,
        }
    }
}

impl From<EncodeError> for Error {
    fn from(err: EncodeError) -> Error {
        Error::Encode(err)
    }
}

impl From<DecodeError> for Error {
    fn from(err: DecodeError) -> Error {
        Error::Decode(err)
    }
}

impl From<VarError> for Error {
    fn from(err: VarError) -> Error {
        Error::Var(err)
    }
}

impl From<IoError> for Error {
    fn from(err: IoError) -> Error {
        Error::Io(err)
    }
}

#[cfg(feature = "storage-sqlite")]
impl From<SqliteError> for Error {
    fn from(err: SqliteError) -> Error {
        Error::Sqlite(err)
    }
}

#[cfg(feature = "storage-redis")]
impl From<RedisError> for Error {
    fn from(err: RedisError) -> Error {
        Error::Redis(err)
    }
}

#[cfg(feature = "storage-zbox")]
impl From<HttpError> for Error {
    fn from(err: HttpError) -> Error {
        Error::Http(err)
    }
}

#[cfg(feature = "storage-zbox")]
impl From<JsonError> for Error {
    fn from(err: JsonError) -> Error {
        Error::Json(err)
    }
}

#[cfg(feature = "storage-zbox-native")]
impl From<ReqwestError> for Error {
    fn from(err: ReqwestError) -> Error {
        Error::Reqwest(err)
    }
}

#[cfg(feature = "storage-zbox-android")]
impl From<JniError> for Error {
    fn from(err: JniError) -> Error {
        Error::Jni(err)
    }
}

impl From<Error> for i32 {
    fn from(e: Error) -> i32 {
        match e {
            Error::RefOverflow => -1000,
            Error::RefUnderflow => -1001,

            Error::InitCrypto => -1010,
            Error::NoAesHardware => -1011,
            Error::Hashing => -1012,
            Error::InvalidCost => -1013,
            Error::InvalidCipher => -1014,
            Error::Encrypt => -1015,
            Error::Decrypt => -1016,

            Error::InvalidUri => -1020,
            Error::InvalidSuperBlk => -1021,
            Error::Corrupted => -1022,
            Error::WrongVersion => -1023,
            Error::NoEntity => -1024,
            Error::NotInSync => -1025,
            Error::RepoOpened => -1026,
            Error::RepoClosed => -1027,
            Error::RepoExists => -1028,

            Error::InTrans => -1030,
            Error::NotInTrans => -1031,
            Error::NoTrans => -1032,
            Error::Uncompleted => -1033,
            Error::InUse => -1034,

            Error::NoContent => -1040,

            Error::InvalidArgument => -1050,
            Error::InvalidPath => -1051,
            Error::NotFound => -1052,
            Error::AlreadyExists => -1053,
            Error::IsRoot => -1054,
            Error::IsDir => -1055,
            Error::IsFile => -1056,
            Error::NotDir => -1057,
            Error::NotFile => -1058,
            Error::NotEmpty => -1059,
            Error::NoVersion => -1060,

            Error::ReadOnly => -1070,
            Error::CannotRead => -1071,
            Error::CannotWrite => -1072,
            Error::NotWrite => -1073,
            Error::NotFinish => -1074,
            Error::Closed => -1075,

            Error::Encode(_) => -2000,
            Error::Decode(_) => -2010,
            Error::Var(_) => -2020,
            Error::Io(_) => -2030,

            #[cfg(feature = "storage-sqlite")]
            Error::Sqlite(_) => -2040,

            #[cfg(feature = "storage-redis")]
            Error::Redis(_) => -2050,

            #[cfg(feature = "storage-zbox")]
            Error::Http(_) => -2060,
            #[cfg(feature = "storage-zbox")]
            Error::HttpStatus(_) => -2061,
            #[cfg(feature = "storage-zbox")]
            Error::Json(_) => -2062,

            #[cfg(feature = "storage-zbox-native")]
            Error::Reqwest(_) => -2063,

            #[cfg(feature = "storage-zbox-android")]
            Error::Jni(_) => -2064,

            #[cfg(target_arch = "wasm32")]
            Error::RequestError => -2065,
        }
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (&Error::RefOverflow, &Error::RefOverflow) => true,
            (&Error::RefUnderflow, &Error::RefUnderflow) => true,

            (&Error::InitCrypto, &Error::InitCrypto) => true,
            (&Error::NoAesHardware, &Error::NoAesHardware) => true,
            (&Error::Hashing, &Error::Hashing) => true,
            (&Error::InvalidCost, &Error::InvalidCost) => true,
            (&Error::InvalidCipher, &Error::InvalidCipher) => true,
            (&Error::Encrypt, &Error::Encrypt) => true,
            (&Error::Decrypt, &Error::Decrypt) => true,

            (&Error::InvalidUri, &Error::InvalidUri) => true,
            (&Error::InvalidSuperBlk, &Error::InvalidSuperBlk) => true,
            (&Error::Corrupted, &Error::Corrupted) => true,
            (&Error::WrongVersion, &Error::WrongVersion) => true,
            (&Error::NoEntity, &Error::NoEntity) => true,
            (&Error::NotInSync, &Error::NotInSync) => true,
            (&Error::RepoOpened, &Error::RepoOpened) => true,
            (&Error::RepoClosed, &Error::RepoClosed) => true,
            (&Error::RepoExists, &Error::RepoExists) => true,

            (&Error::InTrans, &Error::InTrans) => true,
            (&Error::NotInTrans, &Error::NotInTrans) => true,
            (&Error::NoTrans, &Error::NoTrans) => true,
            (&Error::Uncompleted, &Error::Uncompleted) => true,
            (&Error::InUse, &Error::InUse) => true,

            (&Error::NoContent, &Error::NoContent) => true,

            (&Error::InvalidArgument, &Error::InvalidArgument) => true,
            (&Error::InvalidPath, &Error::InvalidPath) => true,
            (&Error::NotFound, &Error::NotFound) => true,
            (&Error::AlreadyExists, &Error::AlreadyExists) => true,
            (&Error::IsRoot, &Error::IsRoot) => true,
            (&Error::IsDir, &Error::IsDir) => true,
            (&Error::IsFile, &Error::IsFile) => true,
            (&Error::NotDir, &Error::NotDir) => true,
            (&Error::NotFile, &Error::NotFile) => true,
            (&Error::NotEmpty, &Error::NotEmpty) => true,
            (&Error::NoVersion, &Error::NoVersion) => true,

            (&Error::ReadOnly, &Error::ReadOnly) => true,
            (&Error::CannotRead, &Error::CannotRead) => true,
            (&Error::CannotWrite, &Error::CannotWrite) => true,
            (&Error::NotWrite, &Error::NotWrite) => true,
            (&Error::NotFinish, &Error::NotFinish) => true,
            (&Error::Closed, &Error::Closed) => true,

            (&Error::Encode(_), &Error::Encode(_)) => true,
            (&Error::Decode(_), &Error::Decode(_)) => true,
            (&Error::Var(_), &Error::Var(_)) => true,
            (&Error::Io(ref a), &Error::Io(ref b)) => a.kind() == b.kind(),

            #[cfg(feature = "storage-sqlite")]
            (&Error::Sqlite(ref a), &Error::Sqlite(ref b)) => a == b,

            #[cfg(feature = "storage-redis")]
            (&Error::Redis(ref a), &Error::Redis(ref b)) => {
                a.kind() == b.kind()
            }

            #[cfg(feature = "storage-zbox")]
            (&Error::HttpStatus(a), &Error::HttpStatus(b)) => a == b,

            #[cfg(feature = "storage-zbox-native")]
            (&Error::Reqwest(ref a), &Error::Reqwest(ref b)) => {
                a.status() == b.status()
            }

            #[cfg(feature = "storage-zbox-android")]
            (&Error::Jni(ref a), &Error::Jni(ref b)) => {
                a.kind().description() == b.kind().description()
            }

            #[cfg(target_arch = "wasm32")]
            (&Error::RequestError, &Error::RequestError) => true,

            (_, _) => false,
        }
    }
}

/// A specialized [`Result`] type for ZboxFS operations.
///
/// See the [`zbox::Error`] for all the  errors.
///
/// [`Result`]: https://doc.rust-lang.org/std/result/enum.Result.html
/// [`zbox::Error`]: enum.Error.html
pub type Result<T> = result::Result<T, Error>;
