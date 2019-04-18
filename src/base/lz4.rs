//! This module is adopted from lz4-rs (https://github.com/bozaro/lz4-rs).
//! Removed unnecessary functions and libc dependency.

use std::cmp;
use std::ffi::CStr;
use std::fmt::{Display, Formatter};
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write};
use std::ptr;
use std::result::Result as StdResult;
use std::str;

#[allow(non_camel_case_types)]
type c_void = std::ffi::c_void;

#[allow(non_camel_case_types)]
type c_char = i8;

#[allow(non_camel_case_types)]
type c_uint = u32;

#[allow(non_camel_case_types)]
type size_t = usize;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct LZ4FCompressionContext(pub *mut c_void);
unsafe impl Send for LZ4FCompressionContext {}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct LZ4FDecompressionContext(pub *mut c_void);
unsafe impl Send for LZ4FDecompressionContext {}

pub type LZ4FErrorCode = size_t;

#[allow(dead_code)]
#[derive(Clone)]
#[repr(u32)]
pub enum BlockSize {
    Default = 0, // Default - 64KB
    Max64KB = 4,
    Max256KB = 5,
    Max1MB = 6,
    Max4MB = 7,
}

impl BlockSize {
    pub fn get_size(&self) -> usize {
        match *self {
            BlockSize::Default | BlockSize::Max64KB => 64 * 1024,
            BlockSize::Max256KB => 256 * 1024,
            BlockSize::Max1MB => 1024 * 1024,
            BlockSize::Max4MB => 4 * 1024 * 1024,
        }
    }
}

#[allow(dead_code)]
#[derive(Clone)]
#[repr(u32)]
pub enum BlockMode {
    Linked = 0,
    Independent,
}

#[derive(Clone)]
#[repr(u32)]
pub enum ContentChecksum {
    NoChecksum = 0,
    ChecksumEnabled,
}

#[repr(C)]
pub struct LZ4FFrameInfo {
    pub block_size_id: BlockSize,
    pub block_mode: BlockMode,
    pub content_checksum_flag: ContentChecksum,
    pub reserved: [c_uint; 5],
}

#[repr(C)]
pub struct LZ4FPreferences {
    pub frame_info: LZ4FFrameInfo,

    // 0 == default (fast mode); values above 16 count as 16
    pub compression_level: c_uint,

    // 1 == always flush : reduce need for tmp buffer
    pub auto_flush: c_uint,

    pub reserved: [c_uint; 4],
}

#[repr(C)]
pub struct LZ4FCompressOptions {
    /* 1 == src content will remain available on future calls
     * to LZ4F_compress(); avoid saving src content within tmp
     * buffer as future dictionary */
    pub stable_src: c_uint,

    pub reserved: [c_uint; 3],
}

#[repr(C)]
pub struct LZ4FDecompressOptions {
    /* guarantee that decompressed data will still be there on next
     * function calls (avoid storage into tmp buffers) */
    pub stable_dst: c_uint,

    pub reserved: [c_uint; 3],
}

pub const LZ4F_VERSION: c_uint = 100;

extern "C" {
    // unsigned    LZ4F_isError(LZ4F_errorCode_t code);
    pub fn LZ4F_isError(code: size_t) -> c_uint;

    // const char* LZ4F_getErrorName(LZ4F_errorCode_t code);
    pub fn LZ4F_getErrorName(code: size_t) -> *const c_char;

    // LZ4F_createCompressionContext() :
    // The first thing to do is to create a compressionContext object, which
    // will be used in all compression operations.
    // This is achieved using LZ4F_createCompressionContext(), which takes as
    // argument a version and an LZ4F_preferences_t structure.
    // The version provided MUST be LZ4F_VERSION. It is intended to track
    // potential version differences between different binaries.
    // The function will provide a pointer to a fully allocated
    // LZ4F_compressionContext_t object.
    // If the result LZ4F_errorCode_t is not zero, there was an error during
    // context creation. Object can release its memory using
    // LZ4F_freeCompressionContext();
    //
    // LZ4F_errorCode_t LZ4F_createCompressionContext(
    //   LZ4F_compressionContext_t* LZ4F_compressionContextPtr,
    //   unsigned version);
    pub fn LZ4F_createCompressionContext(
        ctx: &mut LZ4FCompressionContext,
        version: c_uint,
    ) -> LZ4FErrorCode;

    // LZ4F_errorCode_t LZ4F_freeCompressionContext(
    //   LZ4F_compressionContext_t LZ4F_compressionContext);
    pub fn LZ4F_freeCompressionContext(
        ctx: LZ4FCompressionContext,
    ) -> LZ4FErrorCode;

    // LZ4F_compressBegin(): will write the frame header into dstBuffer.
    // dstBuffer must be large enough to accommodate a header (dstMaxSize).
    // Maximum header size is 19 bytes.
    // The LZ4F_preferences_t structure is optional : you can provide NULL as
    // argument, all preferences will then be set to default.
    // The result of the function is the number of bytes written into dstBuffer
    // for the header or an error code (can be tested using LZ4F_isError())
    //
    // size_t LZ4F_compressBegin(LZ4F_compressionContext_t compressionContext,
    //                           void* dstBuffer,
    //                           size_t dstMaxSize,
    //                           const LZ4F_preferences_t* preferencesPtr);
    pub fn LZ4F_compressBegin(
        ctx: LZ4FCompressionContext,
        dstBuffer: *mut u8,
        dstMaxSize: size_t,
        preferencesPtr: *const LZ4FPreferences,
    ) -> LZ4FErrorCode;

    // LZ4F_compressBound() :
    // Provides the minimum size of Dst buffer given srcSize to handle worst
    // case situations. preferencesPtr is optional : you can provide NULL as
    // argument, all preferences will then be set to default.
    // Note that different preferences will produce in different results.
    //
    // size_t LZ4F_compressBound(size_t srcSize,
    //                           const LZ4F_preferences_t* preferencesPtr);
    pub fn LZ4F_compressBound(
        srcSize: size_t,
        preferencesPtr: *const LZ4FPreferences,
    ) -> LZ4FErrorCode;

    // LZ4F_compressUpdate()
    // LZ4F_compressUpdate() can be called repetitively to compress as much
    // data as necessary. The most important rule is that dstBuffer MUST be
    // large enough (dstMaxSize) to ensure compression completion even in
    // worst case.
    // If this condition is not respected, LZ4F_compress() will fail (result
    // is an errorCode)
    // You can get the minimum value of dstMaxSize by using LZ4F_compressBound()
    // The LZ4F_compressOptions_t structure is optional : you can provide NULL
    // as argument.
    // The result of the function is the number of bytes written into dstBuffer:
    // it can be zero, meaning input data was just buffered. The function
    // outputs an error code if it fails (can be tested using LZ4F_isError())
    //
    // size_t LZ4F_compressUpdate(LZ4F_compressionContext_t compressionContext,
    //                            void* dstBuffer,
    //                            size_t dstMaxSize,
    //                            const void* srcBuffer,
    //                            size_t srcSize,
    //                            const LZ4F_compressOptions_t* compressOptionsPtr);
    pub fn LZ4F_compressUpdate(
        ctx: LZ4FCompressionContext,
        dstBuffer: *mut u8,
        dstMaxSize: size_t,
        srcBuffer: *const u8,
        srcSize: size_t,
        compressOptionsPtr: *const LZ4FCompressOptions,
    ) -> size_t;

    // LZ4F_flush()
    // Should you need to create compressed data immediately, without waiting
    // for a block to be be filled, you can call LZ4_flush(), which will
    // immediately compress any remaining data buffered within
    // compressionContext.
    // The LZ4F_compressOptions_t structure is optional : you can provide NULL
    // as argument. The result of the function is the number of bytes written
    // into dstBuffer (it can be zero, this means there was no data left within
    // compressionContext)
    // The function outputs an error code if it fails (can be tested using
    // LZ4F_isError())
    //
    // size_t LZ4F_flush(LZ4F_compressionContext_t compressionContext,
    //                   void* dstBuffer,
    //                   size_t dstMaxSize,
    //                   const LZ4F_compressOptions_t* compressOptionsPtr);
    pub fn LZ4F_flush(
        ctx: LZ4FCompressionContext,
        dstBuffer: *mut u8,
        dstMaxSize: size_t,
        compressOptionsPtr: *const LZ4FCompressOptions,
    ) -> LZ4FErrorCode;

    // LZ4F_compressEnd()
    // When you want to properly finish the compressed frame, just call
    // LZ4F_compressEnd().
    // It will flush whatever data remained within compressionContext (like
    // LZ4_flush()) but also properly finalize the frame, with an endMark and
    // a checksum.
    // The result of the function is the number of bytes written into dstBuffer
    // (necessarily >= 4 (endMark size))
    // The function outputs an error code if it fails (can be tested using
    // LZ4F_isError())
    // The LZ4F_compressOptions_t structure is optional : you can provide NULL
    // as argument. compressionContext can then be used again, starting with
    // LZ4F_compressBegin().
    //
    // size_t LZ4F_compressEnd(LZ4F_compressionContext_t compressionContext,
    //                         void* dstBuffer,
    //                         size_t dstMaxSize,
    //                         const LZ4F_compressOptions_t* compressOptionsPtr);
    pub fn LZ4F_compressEnd(
        ctx: LZ4FCompressionContext,
        dstBuffer: *mut u8,
        dstMaxSize: size_t,
        compressOptionsPtr: *const LZ4FCompressOptions,
    ) -> LZ4FErrorCode;

    // LZ4F_createDecompressionContext() :
    // The first thing to do is to create a decompressionContext object,
    // which will be used in all decompression operations.
    // This is achieved using LZ4F_createDecompressionContext().
    // The version provided MUST be LZ4F_VERSION. It is intended to track
    // potential version differences between different binaries.
    // The function will provide a pointer to a fully allocated and initialized
    // LZ4F_decompressionContext_t object.
    // If the result LZ4F_errorCode_t is not OK_NoError, there was an error
    // during context creation.
    // Object can release its memory using LZ4F_freeDecompressionContext();
    //
    // LZ4F_errorCode_t
    // LZ4F_createDecompressionContext(LZ4F_decompressionContext_t* ctxPtr,
    //                                                  unsigned version);
    pub fn LZ4F_createDecompressionContext(
        ctx: &mut LZ4FDecompressionContext,
        version: c_uint,
    ) -> LZ4FErrorCode;

    // LZ4F_errorCode_t
    // LZ4F_freeDecompressionContext(LZ4F_decompressionContext_t ctx);
    pub fn LZ4F_freeDecompressionContext(
        ctx: LZ4FDecompressionContext,
    ) -> LZ4FErrorCode;

    // LZ4F_decompress()
    // Call this function repetitively to regenerate data compressed within
    // srcBuffer. The function will attempt to decode *srcSizePtr bytes from
    // srcBuffer, into dstBuffer of maximum size *dstSizePtr.
    //
    // The number of bytes regenerated into dstBuffer will be provided within
    // *dstSizePtr (necessarily <= original value).
    //
    // The number of bytes read from srcBuffer will be provided within
    // *srcSizePtr (necessarily <= original value).
    // If number of bytes read is < number of bytes provided, then
    // decompression operation is not completed. It typically happens when
    // dstBuffer is not large enough to contain all decoded data.
    // LZ4F_decompress() must be called again, starting from where it stopped
    // (srcBuffer + *srcSizePtr)
    // The function will check this condition, and refuse to continue if it is
    // not respected.
    //
    // dstBuffer is supposed to be flushed between each call to the function,
    // since its content will be overwritten.
    // dst arguments can be changed at will with each consecutive call to the
    // function.
    //
    // The function result is an hint of how many srcSize bytes
    // LZ4F_decompress() expects for next call.
    // Schematically, it's the size of the current (or remaining) compressed
    // block + header of next block.
    // Respecting the hint provides some boost to performance, since it does
    // skip intermediate buffers.
    // This is just a hint, you can always provide any srcSize you want.
    // When a frame is fully decoded, the function result will be 0. (no more
    // data expected)
    // If decompression failed, function result is an error code, which can be
    // tested using LZ4F_isError().
    //
    // size_t LZ4F_decompress(LZ4F_decompressionContext_t ctx,
    //                        void* dstBuffer, size_t* dstSizePtr,
    //                        const void* srcBuffer, size_t* srcSizePtr,
    //                        const LZ4F_decompressOptions_t* optionsPtr);
    pub fn LZ4F_decompress(
        ctx: LZ4FDecompressionContext,
        dstBuffer: *mut u8,
        dstSizePtr: &mut size_t,
        srcBuffer: *const u8,
        srcSizePtr: &mut size_t,
        optionsPtr: *const LZ4FDecompressOptions,
    ) -> LZ4FErrorCode;
}

#[derive(Debug)]
pub struct LZ4Error(String);

impl Display for LZ4Error {
    #[inline]
    fn fmt(&self, f: &mut Formatter) -> StdResult<(), ::std::fmt::Error> {
        write!(f, "LZ4 error: {}", &self.0)
    }
}

impl ::std::error::Error for LZ4Error {
    #[inline]
    fn description(&self) -> &str {
        &self.0
    }

    #[inline]
    fn cause(&self) -> Option<&::std::error::Error> {
        None
    }
}

pub fn check_error(code: LZ4FErrorCode) -> IoResult<usize> {
    unsafe {
        if LZ4F_isError(code) != 0 {
            let error_name = LZ4F_getErrorName(code);
            return Err(IoError::new(
                ErrorKind::Other,
                LZ4Error(
                    str::from_utf8(CStr::from_ptr(error_name).to_bytes())
                        .unwrap()
                        .to_string(),
                ),
            ));
        }
    }
    Ok(code as usize)
}

/* =============================================
 * Encoder
 * ============================================= */
struct EncoderContext {
    c: LZ4FCompressionContext,
}

#[derive(Clone)]
pub struct EncoderBuilder {
    block_size: BlockSize,
    block_mode: BlockMode,
    checksum: ContentChecksum,
    // 0 == default (fast mode); values above 16 count as 16;
    // values below 0 count as 0
    level: u32,
    // 1 == always flush (reduce need for tmp buffer)
    auto_flush: bool,
}

pub struct Encoder<W> {
    c: EncoderContext,
    w: W,
    limit: usize,
    buffer: Vec<u8>,
}

impl EncoderBuilder {
    pub fn new() -> Self {
        EncoderBuilder {
            block_size: BlockSize::Default,
            block_mode: BlockMode::Linked,
            checksum: ContentChecksum::ChecksumEnabled,
            level: 0,
            auto_flush: false,
        }
    }

    #[inline]
    pub fn block_size(&mut self, block_size: BlockSize) -> &mut Self {
        self.block_size = block_size;
        self
    }

    #[inline]
    pub fn block_mode(&mut self, block_mode: BlockMode) -> &mut Self {
        self.block_mode = block_mode;
        self
    }

    #[inline]
    pub fn checksum(&mut self, checksum: ContentChecksum) -> &mut Self {
        self.checksum = checksum;
        self
    }

    #[inline]
    pub fn level(&mut self, level: u32) -> &mut Self {
        self.level = level;
        self
    }

    #[inline]
    pub fn auto_flush(&mut self, auto_flush: bool) -> &mut Self {
        self.auto_flush = auto_flush;
        self
    }

    pub fn build<W: Write>(&self, w: W) -> IoResult<Encoder<W>> {
        let block_size = self.block_size.get_size();
        let preferences = LZ4FPreferences {
            frame_info: LZ4FFrameInfo {
                block_size_id: self.block_size.clone(),
                block_mode: self.block_mode.clone(),
                content_checksum_flag: self.checksum.clone(),
                reserved: [0; 5],
            },
            compression_level: self.level,
            auto_flush: if self.auto_flush { 1 } else { 0 },
            reserved: [0; 4],
        };
        let mut encoder = Encoder {
            w,
            c: EncoderContext::new()?,
            limit: block_size,
            buffer: Vec::with_capacity(check_error(unsafe {
                LZ4F_compressBound(block_size as size_t, &preferences)
            })?),
        };
        encoder.write_header(&preferences)?;
        Ok(encoder)
    }
}

impl<W: Write> Encoder<W> {
    fn write_header(&mut self, preferences: &LZ4FPreferences) -> IoResult<()> {
        unsafe {
            let len = check_error(LZ4F_compressBegin(
                self.c.c,
                self.buffer.as_mut_ptr(),
                self.buffer.capacity() as size_t,
                preferences,
            ))?;
            self.buffer.set_len(len);
        }
        self.w.write_all(&self.buffer)
    }

    fn write_end(&mut self) -> IoResult<()> {
        unsafe {
            let len = check_error(LZ4F_compressEnd(
                self.c.c,
                self.buffer.as_mut_ptr(),
                self.buffer.capacity() as size_t,
                ptr::null(),
            ))?;
            self.buffer.set_len(len);
        };
        self.w.write_all(&self.buffer)
    }

    /// This function is used to flag that this session of compression is done
    /// with. The stream is finished up (final bytes are written), and then the
    /// wrapped writer is returned.
    #[inline]
    pub fn finish(mut self) -> (W, IoResult<()>) {
        let result = self.write_end();
        (self.w, result)
    }
}

impl<W: Write> Write for Encoder<W> {
    fn write(&mut self, buffer: &[u8]) -> IoResult<usize> {
        let mut offset = 0;
        while offset < buffer.len() {
            let size = cmp::min(buffer.len() - offset, self.limit);
            unsafe {
                let len = check_error(LZ4F_compressUpdate(
                    self.c.c,
                    self.buffer.as_mut_ptr(),
                    self.buffer.capacity() as size_t,
                    buffer[offset..].as_ptr(),
                    size as size_t,
                    ptr::null(),
                ))?;
                self.buffer.set_len(len);
                self.w.write_all(&self.buffer)?;
            }
            offset += size;
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        loop {
            unsafe {
                let len = check_error(LZ4F_flush(
                    self.c.c,
                    self.buffer.as_mut_ptr(),
                    self.buffer.capacity() as size_t,
                    ptr::null(),
                ))?;
                if len == 0 {
                    break;
                }
                self.buffer.set_len(len);
            };
            self.w.write_all(&self.buffer)?;
        }
        self.w.flush()
    }
}

impl EncoderContext {
    fn new() -> IoResult<EncoderContext> {
        let mut context = LZ4FCompressionContext(ptr::null_mut());
        check_error(unsafe {
            LZ4F_createCompressionContext(&mut context, LZ4F_VERSION)
        })?;
        Ok(EncoderContext { c: context })
    }
}

impl Drop for EncoderContext {
    fn drop(&mut self) {
        unsafe { LZ4F_freeCompressionContext(self.c) };
    }
}

/* =============================================
 * Decoder
 * ============================================= */
const BUFFER_SIZE: usize = 32 * 1024;

struct DecoderContext {
    c: LZ4FDecompressionContext,
}

pub struct Decoder<R> {
    c: DecoderContext,
    r: R,
    buf: Box<[u8]>,
    pos: usize,
    len: usize,
    next: usize,
}

impl<R: Read> Decoder<R> {
    /// Creates a new encoder which will have its output written to the given
    /// output stream. The output stream can be re-acquired by calling
    /// `finish()`
    pub fn new(r: R) -> IoResult<Decoder<R>> {
        Ok(Decoder {
            r,
            c: DecoderContext::new()?,
            buf: vec![0; BUFFER_SIZE].into_boxed_slice(),
            pos: BUFFER_SIZE,
            len: BUFFER_SIZE,
            // Minimal LZ4 stream size
            next: 11,
        })
    }

    #[allow(dead_code)]
    pub fn finish(self) -> (R, IoResult<()>) {
        (
            self.r,
            match self.next {
                0 => Ok(()),
                _ => Err(IoError::new(
                    ErrorKind::Interrupted,
                    "Finish runned before read end of compressed stream",
                )),
            },
        )
    }
}

impl<R: Read> Read for Decoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if self.next == 0 || buf.is_empty() {
            return Ok(0);
        }

        let mut dst_offset: usize = 0;
        while dst_offset == 0 {
            if self.pos >= self.len {
                let need = if self.buf.len() < self.next {
                    self.buf.len()
                } else {
                    self.next
                };
                self.len = self.r.read(&mut self.buf[0..need])?;
                if self.len == 0 {
                    break;
                }
                self.pos = 0;
                self.next -= self.len;
            }
            while (dst_offset < buf.len()) && (self.pos < self.len) {
                let mut src_size = (self.len - self.pos) as size_t;
                let mut dst_size = (buf.len() - dst_offset) as size_t;
                let len = check_error(unsafe {
                    LZ4F_decompress(
                        self.c.c,
                        buf[dst_offset..].as_mut_ptr(),
                        &mut dst_size,
                        self.buf[self.pos..].as_ptr(),
                        &mut src_size,
                        ptr::null(),
                    )
                })?;
                self.pos += src_size as usize;
                dst_offset += dst_size as usize;
                if len == 0 {
                    self.next = 0;
                    return Ok(dst_offset);
                } else if self.next < len {
                    self.next = len;
                }
            }
        }
        Ok(dst_offset)
    }
}

impl DecoderContext {
    fn new() -> IoResult<DecoderContext> {
        let mut context = LZ4FDecompressionContext(ptr::null_mut());
        check_error(unsafe {
            LZ4F_createDecompressionContext(&mut context, LZ4F_VERSION)
        })?;
        Ok(DecoderContext { c: context })
    }
}

impl Drop for DecoderContext {
    fn drop(&mut self) {
        unsafe { LZ4F_freeDecompressionContext(self.c) };
    }
}
