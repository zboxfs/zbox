use std::result::Result as StdResult;
use std::fmt::{self, Debug, Formatter};
use std::cmp::min;
use std::mem;
use std::ptr;
use std::slice;
use std::marker::PhantomData;
use std::ops::Deref;

use serde::{Deserialize, Serialize};
use serde::ser::Serializer;
use serde::de::{self, Deserializer};

use error::{Result, Error};

extern "C" {
    // Initialisation
    // --------------
    fn sodium_init() -> i32;

    // random
    // ----------
    fn randombytes_buf(buf: *mut u8, size: usize);
    fn randombytes_random() -> u32;
    fn randombytes_uniform(upper_bound: u32) -> u32;
    fn randombytes_buf_deterministic(
        buf: *mut u8,
        size: usize,
        seed: *const u8,
    );

    // generic hash
    // -------------
    fn crypto_generichash(
        out: *mut u8,
        outlen: usize,
        inbuf: *const u8,
        inlen: u64,
        key: *const u8,
        keylen: usize,
    ) -> i32;
    fn crypto_generichash_init(
        state: *mut u8,
        key: *const u8,
        keylen: usize,
        outlen: usize,
    ) -> i32;
    fn crypto_generichash_update(
        state: *mut u8,
        inbuf: *const u8,
        inlen: u64,
    ) -> i32;
    fn crypto_generichash_final(
        state: *mut u8,
        out: *mut u8,
        outlen: usize,
    ) -> i32;


    // password hash
    // -------------
    fn crypto_pwhash(
        out: *mut u8,
        outlen: u64,
        passwd: *const u8,
        passwdlen: u64,
        salt: *const u8,
        opslimit: u64,
        memlimit: usize,
        alg: i32,
    ) -> i32;

    // key derivation
    // --------------
    fn crypto_kdf_derive_from_key(
        subkey: *mut u8,
        subkey_len: usize,
        subkey_id: u64,
        ctx: *const u8,
        key: *const u8,
    ) -> i32;

    // XChaCha20-Poly1305 crypto
    // -------------------------
    fn crypto_aead_xchacha20poly1305_ietf_encrypt(
        c: *mut u8,
        clen_p: *const u64,
        m: *const u8,
        mlen: u64,
        ad: *const u8,
        adlen: u64,
        nsec: *const u8,
        npub: *const u8,
        k: *const u8,
    ) -> i32;

    fn crypto_aead_xchacha20poly1305_ietf_decrypt(
        m: *mut u8,
        mlen_p: *const u64,
        nsec: *const u8,
        c: *const u8,
        clen: u64,
        ad: *const u8,
        adlen: u64,
        npub: *const u8,
        k: *const u8,
    ) -> i32;

    // AES256-GCM crypto (hardware only)
    // ---------------------------------
    fn crypto_aead_aes256gcm_is_available() -> i32;

    // nonce extension
    fn crypto_core_hchacha20(
        out: *mut u8,
        inbuf: *const u8,
        k: *const u8,
        c: *const u8,
    ) -> i32;

    fn crypto_aead_aes256gcm_encrypt(
        c: *mut u8,
        clen_p: *const u64,
        m: *const u8,
        mlen: u64,
        ad: *const u8,
        adlen: u64,
        nsec: *const u8,
        npub: *const u8,
        k: *const u8,
    ) -> i32;

    fn crypto_aead_aes256gcm_decrypt(
        m: *mut u8,
        mlen_p: *const u64,
        nsec: *const u8,
        c: *const u8,
        clen: u64,
        ad: *const u8,
        adlen: u64,
        npub: *const u8,
        k: *const u8,
    ) -> i32;

    // Helpers
    // -------
    fn sodium_memzero(pnt: *mut u8, len: usize);
    fn sodium_memcmp(b1: *const u8, b2: *const u8, len: usize) -> i32;
    fn sodium_malloc(size: usize) -> *mut u8;
    fn sodium_free(ptr: *mut u8);
}

/// Safe memory buffer
pub struct SafeBox<T: Sized> {
    ptr: *mut T,
}

impl<T: Sized> SafeBox<T> {
    pub fn new_empty() -> Self {
        unsafe {
            let size = mem::size_of::<T>();
            let ptr = sodium_malloc(size);
            if ptr.is_null() {
                panic!("Secure memory allocation failed");
            }
            sodium_memzero(ptr, size);
            SafeBox { ptr: ptr as *mut T }
        }
    }

    pub fn new() -> Self {
        let mut ret = Self::new_empty();
        unsafe {
            randombytes_buf(ret.as_mut_ptr(), ret.len());
        }
        ret
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr as *mut u8
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    pub fn len(&self) -> usize {
        mem::size_of::<T>()
    }

    /*pub fn reset(&mut self) {
        unsafe {
            sodium_memzero(self.as_mut_ptr(), self.len());
        }
    }*/

    pub fn copy(&mut self, buf: &[u8]) {
        assert!(buf.len() >= self.len());
        unsafe {
            ptr::copy(buf.as_ptr(), self.as_mut_ptr(), self.len());
        }
    }

    pub fn copy_from(&mut self, other: &Key) {
        self.copy(other.as_slice())
    }

    pub fn copy_raw_at(&mut self, buf: *const u8, buf_len: usize, pos: usize) {
        assert!(pos < self.len());
        let len = min(self.len() - pos, buf_len);
        unsafe {
            ptr::copy(buf, self.as_mut_ptr().offset(pos as isize), len);
        }
    }
}

impl<T: Sized> PartialEq for SafeBox<T> {
    fn eq(&self, other: &SafeBox<T>) -> bool {
        unsafe { sodium_memcmp(self.as_ptr(), other.as_ptr(), self.len()) == 0 }
    }
}

impl<T: Sized> fmt::Debug for SafeBox<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "SafeBox(")?;
        unsafe {
            for i in 0..min(4, self.len()) {
                write!(f, "{:x}", *self.as_ptr().offset(i as isize))?;
            }
        }
        if self.len() > 4 {
            write!(f, "..")?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

impl<T: Sized> Clone for SafeBox<T> {
    fn clone(&self) -> Self {
        let mut ret = SafeBox::new_empty();
        ret.copy_raw_at(self.as_ptr(), self.len(), 0);
        ret
    }
}

impl<T: Sized> Drop for SafeBox<T> {
    fn drop(&mut self) {
        unsafe {
            sodium_free(self.as_mut_ptr());
            self.ptr = ptr::null_mut();
        }
    }
}

impl<T: Sized> Serialize for SafeBox<T> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.as_slice())
    }
}

struct SafeBoxVisitor<T> {
    _marker: PhantomData<T>,
}

impl<T> SafeBoxVisitor<T> {
    fn new() -> Self {
        SafeBoxVisitor { _marker: PhantomData::<T> }
    }
}

impl<'de, T> de::Visitor<'de> for SafeBoxVisitor<T> {
    type Value = SafeBox<T>;

    fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "bytes array with length {}", mem::size_of::<T>())
    }

    fn visit_bytes<E>(self, value: &[u8]) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() == mem::size_of::<T>() {
            let mut ret = SafeBox::new_empty();
            ret.copy(value);
            Ok(ret)
        } else {
            Err(de::Error::invalid_length(value.len(), &self))
        }
    }
}

impl<'de, T> Deserialize<'de> for SafeBox<T> {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let visitor = SafeBoxVisitor::new();
        deserializer.deserialize_bytes(visitor)
    }
}

unsafe impl<T: Send> Send for SafeBox<T> {}

unsafe impl<T: Sync> Sync for SafeBox<T> {}

// seed for deterministic random generator
// -------------------------------------
pub const RANDOM_SEED_SIZE: usize = 32;

#[derive(Debug, Default)]
pub struct RandomSeed([u8; RANDOM_SEED_SIZE]);

impl RandomSeed {
    #[allow(dead_code)]
    pub fn new() -> Self {
        let mut seed = Self::default();
        Crypto::random_buf(&mut seed.0);
        seed
    }

    #[allow(dead_code)]
    pub fn from(seed: &[u8; RANDOM_SEED_SIZE]) -> Self {
        RandomSeed(seed.clone())
    }

    pub fn as_ptr(&self) -> *const u8 {
        (&self.0).as_ptr()
    }
}

// hashing constants and types
// ----------------------------
/// Hash value
pub const HASH_SIZE: usize = 32;

#[derive(Clone, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Hash([u8; HASH_SIZE]);

impl Hash {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }
}

impl Deref for Hash {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Hash({}..)", &self.to_string()[..6])
    }
}

impl ToString for Hash {
    fn to_string(&self) -> String {
        let strs: Vec<String> =
            self.0.iter().map(|b| format!("{:x}", b)).collect();
        strs.join("")
    }
}

/// Hash key
pub const HASHKEY_SIZE: usize = 32;
pub type HashKey = SafeBox<[u8; HASHKEY_SIZE]>;

/// Salt size
pub const SALT_SIZE: usize = 16;

/// Salt for password hashing
#[derive(Debug, Clone, Default)]
pub struct Salt([u8; SALT_SIZE]);

impl Salt {
    pub fn new() -> Self {
        let mut salt = Self::default();
        Crypto::random_buf(&mut salt.0);
        salt
    }

    pub fn as_ref(&self) -> &[u8] {
        &self.0
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        let mut salt = [0; SALT_SIZE];
        salt.clone_from_slice(&slice[0..SALT_SIZE]);
        Salt(salt)
    }
}

/// Hash state for multi-part generic hashing, 64 bytes aligned
pub const HASH_STATE_SIZE: usize = 384;
pub type HashState = [u8; HASH_STATE_SIZE];

/// Password hash operation limit.
///
/// It represents a maximum amount of computations to perform. Higher level
/// will require more CPU cycles to compute.
///
/// See https://download.libsodium.org/doc/password_hashing/ for more details.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum OpsLimit {
    Interactive = 4,
    Moderate = 6,
    Sensitive = 8,
}

impl Default for OpsLimit {
    fn default() -> Self {
        OpsLimit::Interactive
    }
}

impl From<u8> for OpsLimit {
    fn from(n: u8) -> Self {
        match n {
            4 => OpsLimit::Interactive,
            6 => OpsLimit::Moderate,
            8 => OpsLimit::Sensitive,
            _ => unimplemented!(),
        }
    }
}

/// Password hash memory limit.
///
/// It represents a maximum amount of memory required to perform password
/// hashing.
///
/// See https://download.libsodium.org/doc/password_hashing/ for more details.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum MemLimit {
    /// 64 MB
    Interactive = 67108864,

    /// 256 MB
    Moderate = 268435456,

    /// 1024 MB
    Sensitive = 1073741824,
}

impl Default for MemLimit {
    fn default() -> Self {
        MemLimit::Interactive
    }
}

/// Password hashing cost consists of [`OpsLimit`] and [`MemLimit`].
///
/// [`OpsLimit`]: enum.OpsLimit.html
/// [`MemLimit`]: enum.MemLimit.html
#[derive(Debug, Clone, Copy, Default)]
pub struct Cost {
    pub ops_limit: OpsLimit,
    pub mem_limit: MemLimit,
}

impl Cost {
    pub const BYTES_LEN: usize = 1;

    pub fn new(ops_limit: OpsLimit, mem_limit: MemLimit) -> Self {
        Cost {
            ops_limit,
            mem_limit,
        }
    }

    pub fn to_u8(&self) -> u8 {
        let ops_limit = match self.ops_limit {
            OpsLimit::Interactive => 0u8,
            OpsLimit::Moderate => 1u8,
            OpsLimit::Sensitive => 2u8,
        };
        let mem_limit = match self.mem_limit {
            MemLimit::Interactive => 0u8,
            MemLimit::Moderate => 1u8,
            MemLimit::Sensitive => 2u8,
        };
        ops_limit | (mem_limit << 4)
    }

    pub fn from_u8(c: u8) -> Result<Self> {
        Ok(Cost {
            ops_limit: match c & 0x0f {
                0 => OpsLimit::Interactive,
                1 => OpsLimit::Moderate,
                2 => OpsLimit::Sensitive,
                _ => return Err(Error::InvalidCost),
            },
            mem_limit: match c >> 4 {
                0 => MemLimit::Interactive,
                1 => MemLimit::Moderate,
                2 => MemLimit::Sensitive,
                _ => return Err(Error::InvalidCost),
            },
        })
    }
}

/// Password hash value
#[derive(Debug, Default)]
pub struct PwdHash {
    pub salt: Salt,
    pub cost: Cost,
    pub hash: Hash,
}

impl PwdHash {
    pub fn new() -> Self {
        PwdHash::default()
    }
}

impl Drop for PwdHash {
    fn drop(&mut self) {
        unsafe {
            sodium_memzero(self.hash.as_mut_ptr(), HASH_SIZE);
        }
    }
}

// AEAE crypto constants and types
// --------------------------------
/// Crypto key
pub const KEY_SIZE: usize = 32;
pub type Key = SafeBox<[u8; KEY_SIZE]>;

impl Key {
    pub fn from_slice(slice: &[u8]) -> Self {
        let mut ret = Self::default();
        ret.copy(slice);
        ret
    }
}

impl Default for Key {
    fn default() -> Self {
        Self::new_empty()
    }
}

/// Crypto cipher primitivies.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Cipher {
    /// XChaCha20-Poly1305
    Xchacha,

    /// AES256-GCM, hardware only
    Aes,
}

impl Cipher {
    pub(crate) const BYTES_LEN: usize = 1;

    pub(crate) fn to_u8(&self) -> u8 {
        match *self {
            Cipher::Aes => 0,
            Cipher::Xchacha => 1,
        }
    }

    pub(crate) fn from_u8(s: u8) -> Result<Self> {
        Ok(match s {
            0 => Cipher::Aes,
            1 => Cipher::Xchacha,
            _ => return Err(Error::InvalidCipher),
        })
    }
}

impl Default for Cipher {
    fn default() -> Self {
        Cipher::Xchacha
    }
}

/// Authentication tag size
const ATAG_SIZE: usize = 16;

/// Nonce
/// Note: AES nonce is extended, original is only 12 bytes. Since AES nonce
/// is larger than Xchacha, we combine them into a single nonce type.
const AES_NONCE_SIZE: usize = 28;
const XCHACHA_NONCE_SIZE: usize = 24;
type Nonce = [u8; AES_NONCE_SIZE];

// encrypt/decrypt function type
type EncryptFn = unsafe extern "C" fn(c: *mut u8,
                                      clen_p: *const u64,
                                      m: *const u8,
                                      mlen: u64,
                                      ad: *const u8,
                                      adlen: u64,
                                      nsec: *const u8,
                                      npub: *const u8,
                                      k: *const u8)
                                      -> i32;
type DecryptFn = unsafe extern "C" fn(m: *mut u8,
                                      mlen_p: *const u64,
                                      nsec: *const u8,
                                      c: *const u8,
                                      clen: u64,
                                      ad: *const u8,
                                      adlen: u64,
                                      npub: *const u8,
                                      k: *const u8)
                                      -> i32;

/// Crypto
#[derive(Debug, Clone)]
pub struct Crypto {
    pub cost: Cost,
    pub cipher: Cipher,
    enc_fn: EncryptFn, // encrypt function pointer
    dec_fn: DecryptFn, // decrypt function pointer
}

impl Crypto {
    // nonce extension const
    const NONCE_EXT_CONST: [u8; 16] = [
        0x32,
        0xb9,
        0xa5,
        0xb8,
        0xb1,
        0x96,
        0x83,
        0x85,
        0xa3,
        0x4e,
        0x47,
        0x97,
        0x0d,
        0x82,
        0xc1,
        0x6d,
    ];

    /// Initialise libsodium
    pub fn init() -> Result<()> {
        unsafe {
            if sodium_init() < 0 {
                return Err(Error::InitCrypto);
            }
        }
        Ok(())
    }

    /// Create new crypto
    pub fn new(cost: Cost, cipher: Cipher) -> Result<Self> {
        match cipher {
            Cipher::Xchacha => {
                Ok(Crypto {
                    cost,
                    cipher,
                    enc_fn: crypto_aead_xchacha20poly1305_ietf_encrypt,
                    dec_fn: crypto_aead_xchacha20poly1305_ietf_decrypt,
                })
            }
            Cipher::Aes => {
                if !Crypto::is_aes_hardware_available() {
                    return Err(Error::NoAesHardware);
                }

                Ok(Crypto {
                    cost,
                    cipher,
                    enc_fn: crypto_aead_aes256gcm_encrypt,
                    dec_fn: crypto_aead_aes256gcm_decrypt,
                })
            }
        }
    }

    // ---------
    // Random
    // ---------
    /// Fill buffer with raondom data
    pub fn random_buf(buf: &mut [u8]) {
        unsafe {
            randombytes_buf(buf.as_mut_ptr(), buf.len());
        }
    }

    /// Fill buffer with random data determined by seed
    #[allow(dead_code)]
    pub fn random_buf_deterministic(buf: &mut [u8], seed: &RandomSeed) {
        unsafe {
            randombytes_buf_deterministic(
                buf.as_mut_ptr(),
                buf.len(),
                seed.as_ptr(),
            );
        }
    }

    /// Generate a random usize integer
    #[allow(dead_code)]
    pub fn random_usize() -> usize {
        unsafe { randombytes_random() as usize }
    }

    /// Generate a random u32 integer between [0, upper_bound)
    #[allow(dead_code)]
    pub fn random_u32(upper_bound: u32) -> u32 {
        unsafe { randombytes_uniform(upper_bound) }
    }

    // -------------
    // Generic Hash
    // -------------
    /// Generic purpose hashing on raw pointer
    pub fn hash_raw(
        inbuf: *const u8,
        len: usize,
        key: *const u8,
        keylen: usize,
    ) -> Hash {
        let mut ret = Hash::new();
        unsafe {
            match crypto_generichash(
                ret.as_mut_ptr(),
                HASH_SIZE,
                inbuf,
                len as u64,
                key,
                keylen,
            ) {
                0 => ret,
                _ => unreachable!(),
            }
        }
    }

    /// Generic purpose hashing with key
    pub fn hash_with_key(inbuf: &[u8], key: &HashKey) -> Hash {
        Crypto::hash_raw(
            inbuf.as_ptr(),
            inbuf.len(),
            key.as_ptr(),
            HASHKEY_SIZE,
        )
    }

    /// Generic purpose hashing without key
    pub fn hash(inbuf: &[u8]) -> Hash {
        Crypto::hash_raw(inbuf.as_ptr(), inbuf.len(), ptr::null(), 0)
    }

    /// Initialise hash state for multi-part hashing.
    pub fn hash_init() -> HashState {
        let mut state: HashState = [0u8; HASH_STATE_SIZE];
        unsafe {
            match crypto_generichash_init(
                (&mut state).as_mut_ptr(),
                ptr::null(),
                0,
                HASH_SIZE,
            ) {
                0 => state,
                _ => unreachable!(),
            }
        }
    }

    /// Processing a chunk of the message, update hash state.
    pub fn hash_update(state: &mut HashState, inbuf: &[u8]) {
        unsafe {
            match crypto_generichash_update(
                state.as_mut_ptr(),
                inbuf.as_ptr(),
                inbuf.len() as u64,
            ) {
                0 => (),
                _ => unreachable!(),
            };
        }
    }

    /// Finanlise multi-part hashing.
    pub fn hash_final(state: &mut HashState) -> Hash {
        let mut ret = Hash::new();
        unsafe {
            match crypto_generichash_final(
                state.as_mut_ptr(),
                ret.as_mut_ptr(),
                HASH_SIZE,
            ) {
                0 => ret,
                _ => unreachable!(),
            }
        }
    }

    // -------------
    // Password Hash
    // -------------
    /// Password hashing
    pub fn hash_pwd(&self, passwd: &str, salt: &Salt) -> Result<PwdHash> {
        let mut pwdhash = PwdHash::new();

        pwdhash.salt = salt.clone();
        pwdhash.cost = self.cost;

        unsafe {
            match crypto_pwhash(
                pwdhash.hash.as_mut_ptr(),
                HASH_SIZE as u64,
                passwd.as_ptr(),
                passwd.len() as u64,
                &pwdhash.salt.0 as *const u8,
                pwdhash.cost.ops_limit as u64,
                pwdhash.cost.mem_limit as usize,
                1,
            ) {
                0 => Ok(pwdhash),
                _ => Err(Error::Hashing),
            }
        }
    }

    // --------------
    // Key derivation
    // --------------
    /// Key derivation
    pub fn derive_from_key(key: &Key, subkey_id: u64) -> Result<Key> {
        let mut subkey = Key::new_empty();
        let ctx = b"ZBox_Key"; // 8 bytes const string

        unsafe {
            match crypto_kdf_derive_from_key(
                subkey.as_mut_ptr(),
                KEY_SIZE,
                subkey_id,
                ctx.as_ptr(),
                key.as_ptr(),
            ) {
                0 => Ok(subkey),
                _ => Err(Error::Hashing),
            }
        }
    }

    // -------------
    // AEAD crypto
    // -------------
    #[inline]
    fn nonce_size(&self) -> usize {
        match self.cipher {
            Cipher::Xchacha => XCHACHA_NONCE_SIZE,
            Cipher::Aes => AES_NONCE_SIZE,
        }
    }

    // extend nonce and key to sub-nonce and sub-key, used for AES cipher only
    fn extend_nonce(&self, nonce: *const u8, key: &Key) -> (*const u8, Key) {
        assert_eq!(self.cipher, Cipher::Aes);
        let mut subkey = Key::new_empty();
        unsafe {
            let subnonce = nonce.offset(16);
            crypto_core_hchacha20(
                subkey.as_mut_ptr(),
                nonce,
                key.as_ptr(),
                Crypto::NONCE_EXT_CONST.as_ptr(),
            );
            (subnonce, subkey)
        }
    }

    // Check if AES is supported by hardware
    #[inline]
    pub fn is_aes_hardware_available() -> bool {
        unsafe { crypto_aead_aes256gcm_is_available() == 1 }
    }

    #[inline]
    pub fn encrypted_len(&self, msglen: usize) -> usize {
        self.nonce_size() + ATAG_SIZE + msglen
    }

    #[inline]
    pub fn decrypted_len(&self, ctxt_len: usize) -> usize {
        ctxt_len - self.nonce_size() - ATAG_SIZE
    }

    /// Encrypt message with specified key
    pub fn encrypt_raw(
        &self,
        ctxt: &mut [u8],
        msg: &[u8],
        key: &Key,
        ad: &[u8],
    ) -> Result<usize> {
        let nonce_size = self.nonce_size();
        let p_ctxt = ctxt.as_mut_ptr();
        let mut clen: u64 = 0;

        // AES extended nonce is longer than Xchacha, so we can use it
        // for both of the ciphers
        let mut nonce: Nonce = [0u8; AES_NONCE_SIZE];
        Crypto::random_buf(&mut nonce);

        let result = match self.cipher {
            Cipher::Xchacha => unsafe {
                (self.enc_fn)(
                    p_ctxt.offset(nonce_size as isize),
                    &mut clen as *mut u64,
                    msg.as_ptr() as *const u8,
                    msg.len() as u64,
                    ad.as_ptr() as *const u8,
                    ad.len() as u64,
                    ptr::null(),
                    nonce.as_ptr(),
                    key.as_ptr(),
                )
            },
            Cipher::Aes => {
                let (subnonce, subkey) = self.extend_nonce(nonce.as_ptr(), key);
                unsafe {
                    (self.enc_fn)(
                        p_ctxt.offset(nonce_size as isize),
                        &mut clen as *mut u64,
                        msg.as_ptr() as *const u8,
                        msg.len() as u64,
                        ad.as_ptr() as *const u8,
                        ad.len() as u64,
                        ptr::null(),
                        subnonce,
                        subkey.as_ptr(),
                    )
                }
            }
        };

        match result {
            0 => {
                // add nonce before encrypted text
                unsafe {
                    ptr::copy(nonce.as_ptr(), p_ctxt, nonce_size);
                }
                Ok(clen as usize + nonce_size)
            }
            _ => Err(Error::Encrypt),
        }
    }

    pub fn encrypt_with_ad(
        &self,
        msg: &[u8],
        key: &Key,
        ad: &[u8],
    ) -> Result<Vec<u8>> {
        let mut ctxt = vec![0u8; self.encrypted_len(msg.len())];
        let enc_len = self.encrypt_raw(&mut ctxt, msg, key, ad)?;
        unsafe {
            ctxt.set_len(enc_len);
        }
        Ok(ctxt)
    }

    #[inline]
    pub fn encrypt(&self, msg: &[u8], key: &Key) -> Result<Vec<u8>> {
        self.encrypt_with_ad(msg, key, &[0u8; 0])
    }

    pub fn encrypt_to(
        &self,
        dst: &mut [u8],
        msg: &[u8],
        key: &Key,
    ) -> Result<usize> {
        self.encrypt_raw(dst, msg, key, &[0u8; 0])
    }

    /// Decrypt message with specified key
    pub fn decrypt_raw(
        &self,
        msg: &mut [u8],
        ctxt: &[u8],
        key: &Key,
        ad: &[u8],
    ) -> Result<usize> {
        let mut msglen = msg.len() as u64;
        let nonce_size = self.nonce_size();
        let nonce = &ctxt[0..nonce_size];

        let result = match self.cipher {
            Cipher::Xchacha => unsafe {
                (self.dec_fn)(
                    msg.as_mut_ptr(),
                    &mut msglen as *mut u64,
                    ptr::null(),
                    ctxt.as_ptr().offset(nonce_size as isize),
                    (ctxt.len() - nonce_size) as u64,
                    ad.as_ptr() as *const u8,
                    ad.len() as u64,
                    nonce.as_ptr(),
                    key.as_ptr(),
                )
            },
            Cipher::Aes => {
                let (subnonce, subkey) = self.extend_nonce(nonce.as_ptr(), key);
                unsafe {
                    (self.dec_fn)(
                        msg.as_mut_ptr(),
                        &mut msglen as *mut u64,
                        ptr::null(),
                        ctxt.as_ptr().offset(nonce_size as isize),
                        (ctxt.len() - nonce_size) as u64,
                        ad.as_ptr() as *const u8,
                        ad.len() as u64,
                        subnonce,
                        subkey.as_ptr(),
                    )
                }
            }
        };
        match result {
            0 => Ok(msglen as usize),
            _ => Err(Error::Decrypt),
        }
    }

    pub fn decrypt_with_ad(
        &self,
        ctxt: &[u8],
        key: &Key,
        ad: &[u8],
    ) -> Result<Vec<u8>> {
        let mut msg = vec![0u8; self.decrypted_len(ctxt.len())];
        let dec_len = self.decrypt_raw(&mut msg, ctxt, key, ad)?;
        unsafe {
            msg.set_len(dec_len);
        }
        Ok(msg)
    }

    #[inline]
    pub fn decrypt(&self, ctxt: &[u8], key: &Key) -> Result<Vec<u8>> {
        self.decrypt_with_ad(ctxt, key, &[0u8; 0])
    }

    pub fn decrypt_to(
        &self,
        dst: &mut [u8],
        ctxt: &[u8],
        key: &Key,
    ) -> Result<usize> {
        self.decrypt_raw(dst, ctxt, key, &[0u8; 0])
    }
}

impl Default for Crypto {
    fn default() -> Self {
        Crypto {
            cost: Cost::default(),
            cipher: Cipher::default(),
            enc_fn: crypto_aead_xchacha20poly1305_ietf_encrypt,
            dec_fn: crypto_aead_xchacha20poly1305_ietf_decrypt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enc_dec() {
        Crypto::init().unwrap();

        let crypto = Crypto::default();
        const LEN: usize = 10;
        let msg = vec![3u8; LEN];
        let key = Key::new();
        let ad = vec![42u8; 4];

        // encryption
        let out = crypto.encrypt_with_ad(&msg, &key, &ad).unwrap();
        assert!(out.len() > 0);

        // decryption
        let ret = crypto.decrypt_with_ad(&out, &key, &ad);
        assert!(ret.is_ok());
        assert_eq!(ret.unwrap(), msg);

        // any changes to cipher text should fail decryption
        let mut ctxt = out.clone();
        if ctxt[3] == 1 {
            ctxt[3] = 2;
        } else {
            ctxt[3] = 1;
        }
        assert!(crypto.decrypt_with_ad(&ctxt, &key, &ad).is_err());
    }
}
