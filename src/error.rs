use std::error::Error as StdError;
use std::result::Result as StdResult;
use std::io::Error as IoError;
use std::env::VarError;
use std::fmt::{self, Display, Formatter};

use rmp_serde::encode::Error as EncodeError;
use rmp_serde::decode::Error as DecodeError;

use base::crypto::Error as CryptoError;

#[derive(Debug)]
pub enum Error {
    Lock,
    RefOverflow,
    RefUnderflow,

    InvalidUri,
    InvalidSuperBlk,
    Corrupted,
    Opened,
    WrongVersion,
    NoEntity,

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
    NotWrite,
    NotFinish,

    Crypto(CryptoError),
    Encode(EncodeError),
    Decode(DecodeError),
    Var(VarError),
    Io(IoError),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Error::Lock => write!(f, "Lock failed"),
            Error::RefOverflow => write!(f, "Refcnt overflow"),
            Error::RefUnderflow => write!(f, "Refcnt underflow"),

            Error::InvalidUri => write!(f, "Invalid Uri"),
            Error::InvalidSuperBlk => write!(f, "Invalid super block"),
            Error::Corrupted => write!(f, "Volume is corrupted"),
            Error::Opened => write!(f, "Volume is opened"),
            Error::WrongVersion => write!(f, "Version not match"),
            Error::NoEntity => write!(f, "Entity not found"),

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
            Error::NotWrite => write!(f, "File does not write yet"),
            Error::NotFinish => write!(f, "File does not finish yet"),

            Error::Crypto(ref err) => err.fmt(f),
            Error::Encode(ref err) => err.fmt(f),
            Error::Decode(ref err) => err.fmt(f),
            Error::Var(ref err) => err.fmt(f),
            Error::Io(ref err) => err.fmt(f),
        }
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Lock => "Lock failed",
            Error::RefOverflow => "Refcnt overflow",
            Error::RefUnderflow => "Refcnt underflow",

            Error::InvalidUri => "Invalid Uri",
            Error::InvalidSuperBlk => "Invalid super block",
            Error::Corrupted => "Volume is corrupted",
            Error::Opened => "Volume is opened",
            Error::WrongVersion => "Version not match",
            Error::NoEntity => "Entity not found",

            Error::InTrans => "Already in transaction",
            Error::NotInTrans => "Not in transaction",
            Error::NoTrans => "Transaction not found",
            Error::Uncompleted => "Transaction uncompleted",
            Error::InUse => "Entity is in use",

            Error::NoContent => "Content not found",

            Error::InvalidArgument => "Invalid argument",
            Error::InvalidPath => "Invalid path",
            Error::NotFound => "File not found",
            Error::AlreadyExists => "File already exists",
            Error::IsRoot => "File is root",
            Error::IsDir => "Path is dir",
            Error::IsFile => "Path is file",
            Error::NotDir => "Path is not dir",
            Error::NotFile => "Path is not file",
            Error::NotEmpty => "Directory is not empty",
            Error::NoVersion => "File has no version",

            Error::ReadOnly => "Opened as read only",
            Error::NotWrite => "File does not write yet",
            Error::NotFinish => "File does not finish yet",

            Error::Crypto(ref err) => err.description(),
            Error::Encode(ref err) => err.description(),
            Error::Decode(ref err) => err.description(),
            Error::Var(ref err) => err.description(),
            Error::Io(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            Error::Crypto(ref err) => Some(err),
            Error::Encode(ref err) => Some(err),
            Error::Decode(ref err) => Some(err),
            Error::Var(ref err) => Some(err),
            Error::Io(ref err) => Some(err),
            _ => None,
        }
    }
}

impl From<CryptoError> for Error {
    fn from(err: CryptoError) -> Error {
        Error::Crypto(err)
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

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (&Error::Lock, &Error::Lock) => true,
            (&Error::RefOverflow, &Error::RefOverflow) => true,
            (&Error::RefUnderflow, &Error::RefUnderflow) => true,
            (&Error::InvalidUri, &Error::InvalidUri) => true,
            (&Error::InvalidSuperBlk, &Error::InvalidSuperBlk) => true,
            (&Error::Corrupted, &Error::Corrupted) => true,
            (&Error::Opened, &Error::Opened) => true,
            (&Error::WrongVersion, &Error::WrongVersion) => true,
            (&Error::NoEntity, &Error::NoEntity) => true,
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
            (&Error::NotWrite, &Error::NotWrite) => true,
            (&Error::NotFinish, &Error::NotFinish) => true,
            (&Error::Crypto(ref a), &Error::Crypto(ref b)) => *a == *b,
            (&Error::Encode(_), &Error::Encode(_)) => true,
            (&Error::Decode(_), &Error::Decode(_)) => true,
            (&Error::Var(_), &Error::Var(_)) => true,
            (&Error::Io(ref a), &Error::Io(ref b)) => a.kind() == b.kind(),
            (_, _) => false,
        }
    }
}
pub type Result<T> = StdResult<T, Error>;
