package io.zbox;

import java.nio.ByteBuffer;

public final class File {
    private long rustObj = 0;

    public void close() {
        if (this.rustObj != 0) {
            this.jniTakeRustObj();
        }
    }

    @Override
    public void finalize() {
        this.close();
    }

    public Metadata metadata() throws ZboxException {
        return this.jniMetadata();
    }

    public Version[] history() throws ZboxException {
        return this.jniHistory();
    }

    public long currVersion() throws ZboxException {
        return this.jniCurrVersion();
    }

    public VersionReader versionReader(long verNum) throws ZboxException {
        return this.jniVersionReader(verNum);
    }

    public void finish() throws ZboxException {
        this.jniFinish();
    }

    public void writeOnce(ByteBuffer buf) throws ZboxException {
        this.jniWriteOnce(buf);
    }

    public void setLen(long len) throws ZboxException {
        this.jniSetLen(len);
    }

    public long read(ByteBuffer dst) throws ZboxException {
        return this.jniRead(dst);
    }

    public long write(ByteBuffer buf) throws ZboxException {
        return this.jniWrite(buf);
    }

    public long seek(long offset, SeekFrom whence) throws ZboxException {
        return this.jniSeek(offset, whence.getValue());
    }

    private native void jniTakeRustObj();
    private native Metadata jniMetadata();
    private native Version[] jniHistory();
    private native long jniCurrVersion();
    private native VersionReader jniVersionReader(long verNum);
    private native void jniFinish();
    private native void jniWriteOnce(ByteBuffer buf);
    private native void jniSetLen(long len);
    private native long jniRead(ByteBuffer dst);
    private native long jniWrite(ByteBuffer buf);
    private native long jniSeek(long offset, int whence);
}
