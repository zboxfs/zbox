package io.zbox;

public final class RepoOpener {
    private long rustObj = 0;

    public RepoOpener() {
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

    public void opsLimit(OpsLimit limit) {
        this.jniOpsLimit(limit.getValue());
    }

    public void memLimit(MemLimit limit) {
        this.jniMemLimit(limit.getValue());
    }

    public void cipher(Cipher cipher) {
        this.jniCipher(cipher.getValue());
    }

    public void create(boolean create) {
        this.jniCreate(create);
    }

    public void createNew(boolean createNew) {
        this.jniCreateNew(createNew);
    }

    public void compress(boolean compress) {
        this.jniCompress(compress);
    }

    public void versionLimit(int limit) {
        this.jniVersionLimit(limit);
    }

    public void dedupChunk(boolean dedup) {
        this.jniDedupChunk(dedup);
    }

    public void readOnly(boolean readOnly) {
        this.jniReadOnly(readOnly);
    }

    public Repo open(String uri, String pwd) throws ZboxException {
        Repo repo = this.jniOpen(uri, pwd);
        return repo;
    }

    private native void jniSetRustObj();
    private native void jniTakeRustObj();
    private native void jniOpsLimit(int limit);
    private native void jniMemLimit(int limit);
    private native void jniCipher(int cipher);
    private native void jniCreate(boolean create);
    private native void jniCreateNew(boolean createNew);
    private native void jniCompress(boolean compress);
    private native void jniVersionLimit(int limit);
    private native void jniDedupChunk(boolean dedup);
    private native void jniReadOnly(boolean readOnly);
    private native Repo jniOpen(String uri, String pwd);
}

