package io.zbox;

public final class Repo {
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

    public static boolean exists(String uri) throws ZboxException {
        return jniExists(uri);
    }

    public RepoInfo info() {
        return this.jniInfo();
    }

    public void resetPassword(String oldPwd, String newPwd, OpsLimit opsLimit, MemLimit memLimit) throws ZboxException {
        this.jniResetPassword(oldPwd, newPwd, opsLimit.getValue(), memLimit.getValue());
    }

    public boolean pathExists(String path) {
        return this.jniPathExists(path);
    }

    public boolean isFile(String path) {
        return this.jniIsFile(path);
    }

    public boolean isDir(String path) {
        return this.jniIsDir(path);
    }

    public File createFile(String path) throws ZboxException {
        return this.jniCreateFile(path);
    }

    public File openFile(String path) throws ZboxException {
        return this.jniOpenFile(path);
    }

    public void createDir(String path) throws ZboxException {
        this.jniCreateDir(path);
    }

    public void createDirAll(String path) throws ZboxException {
        this.jniCreateDirAll(path);
    }

    public DirEntry[] readDir(String path) throws ZboxException {
        return this.jniReadDir(path);
    }

    public Metadata metadata(String path) throws ZboxException {
        return this.jniMetadata(path);
    }

    public Version[] history(String path) throws ZboxException {
        return this.jniHistory(path);
    }

    public void copy(String from, String to) throws ZboxException {
        this.jniCopy(from, to);
    }

    public void removeFile(String path) throws ZboxException {
        this.jniRemoveFile(path);
    }

    public void removeDir(String path) throws ZboxException {
        this.jniRemoveDir(path);
    }

    public void removeDirAll(String path) throws ZboxException {
        this.jniRemoveDirAll(path);
    }

    public void rename(String from, String to) throws ZboxException {
        this.jniRename(from, to);
    }

    private native void jniTakeRustObj();
    private native static boolean jniExists(String uri);
    private native RepoInfo jniInfo();
    private native void jniResetPassword(String oldPwd, String newPwd, int opsLimit, int memLimit);
    private native boolean jniPathExists(String path);
    private native boolean jniIsFile(String path);
    private native boolean jniIsDir(String path);
    private native File jniCreateFile(String path);
    private native File jniOpenFile(String path);
    private native void jniCreateDir(String path);
    private native void jniCreateDirAll(String path);
    private native DirEntry[] jniReadDir(String path);
    private native Metadata jniMetadata(String path);
    private native Version[] jniHistory(String path);
    private native void jniCopy(String from, String to);
    private native void jniRemoveFile(String path);
    private native void jniRemoveDir(String path);
    private native void jniRemoveDirAll(String path);
    private native void jniRename(String from, String to);
}
