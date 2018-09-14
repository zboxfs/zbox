package io.zbox;

public final class OpenOptions {
    private long rustObj = 0;

    public OpenOptions() {
        this.jniSetRustObj();
    }

    public void close() {
        if (this.rustObj != 0) {
            this.jniTakeRustObj();
        }
    }

    @Override
    public void finalize() {
        this.close();
    }

    public void read(boolean read) {
        this.jniRead(read);
    }

    public void write(boolean write) {
        this.jniWrite(write);
    }

    public void append(boolean append) {
        this.jniAppend(append);
    }

    public void truncate(boolean truncate) {
        this.jniTruncate(truncate);
    }

    public void create(boolean create) {
        this.jniCreate(create);
    }

    public void createNew(boolean createNew) {
        this.jniCreateNew(createNew);
    }

    public void versionLimit(int limit) {
        this.jniVersionLimit(limit);
    }

    public void dedupChunk(boolean dedup) {
        this.jniDedupChunk(dedup);
    }

    public File open(Repo repo, String path) throws ZboxException {
        File file = this.jniOpen(repo, path);
        return file;
    }

    private native void jniSetRustObj();
    private native void jniTakeRustObj();
    private native void jniRead(boolean read);
    private native void jniWrite(boolean write);
    private native void jniAppend(boolean append);
    private native void jniTruncate(boolean truncate);
    private native void jniCreate(boolean create);
    private native void jniCreateNew(boolean createNew);
    private native void jniVersionLimit(int limit);
    private native void jniDedupChunk(boolean dedup);
    private native File jniOpen(Repo repo, String path);
}
