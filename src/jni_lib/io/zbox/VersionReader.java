package io.zbox;

import java.nio.ByteBuffer;

public final class VersionReader {
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

    public long read(ByteBuffer dst) throws ZboxException {
        return this.jniRead(dst);
    }

    public long seek(long offset, SeekFrom whence) throws ZboxException {
        return this.jniSeek(offset, whence.getValue());
    }

    private native void jniTakeRustObj();
    private native long jniRead(ByteBuffer dst);
    private native long jniSeek(long offset, int whence);
}


