import java.nio.ByteBuffer;
import io.zbox.*;

public class JniTest {
    public static void main(String[] args) {
        Env.init();

        try {
            System.out.println(Repo.exists("mem://foo"));
        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }

        RepoOpener opener = new RepoOpener();
        opener.compress(true);
        opener.create(true);
        Repo repo = new Repo();
        try {
            repo = opener.open("mem://foo", "pwd");
        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }
        opener.close();

        boolean result = repo.pathExists("/");
        boolean result2 = repo.pathExists("/xxx");

        RepoInfo info = repo.info();
        System.out.println(info.volumeId.length);
        System.out.println(info.version);
        System.out.println(info.uri);
        System.out.println(info.opsLimit);
        System.out.println(info.memLimit);
        System.out.println(info.cipher);
        System.out.println(info.compress);
        System.out.println(info.versionLimit);
        System.out.println(info.dedupChunk);
        System.out.println(info.isReadOnly);
        System.out.println(info.createdAt);

        boolean result3 = repo.isFile("/");
        boolean result4 = repo.isDir("/");
        System.out.println(result3);
        System.out.println(result4);

        OpenOptions opts = new OpenOptions();
        opts.write(true);
        opts.create(true);
        File file = new File();
        try {
            file = opts.open(repo, "/file");
            file.close();
        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }
        opts.close();

        try {
            File file2 = repo.createFile("/file2");
            file2.close();
        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }

        try {
            DirEntry[] dirs = repo.readDir("/");
            System.out.println(dirs.length);
            System.out.println(dirs[0].fileName);
            System.out.println(dirs[1].fileName);
            System.out.println(dirs[0].metadata.fileType);
            System.out.println(dirs[0].metadata.createdAt);
        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }

        try {
            Version[] vers = repo.history("/file");
            System.out.println(vers.length);
        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }

        try {
            repo.copy("/file", "/file3");
        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }

        try {
            repo.removeFile("/file3");
        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }

        try {
            repo.createDirAll("/dir1/dir2");
        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }

        try {
            repo.removeDirAll("/dir1");
        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }

        try {
            repo.rename("/file", "/file4");
        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }

        try {
            File file5 = repo.createFile("/file5");
            Metadata meta = file5.metadata();
            System.out.println(meta);

            ByteBuffer buf = ByteBuffer.allocateDirect(10);
            buf.put("abc".getBytes());
            file5.writeOnce(buf);

            file5.seek(0, SeekFrom.START);
            ByteBuffer buf2 = ByteBuffer.allocateDirect(10);
            buf2.put("def".getBytes());
            file5.writeOnce(buf2);
            System.out.println(file5.currVersion());

            file5.seek(0, SeekFrom.START);
            ByteBuffer dst = ByteBuffer.allocateDirect(10);
            long read = file5.read(dst);
            System.out.println(read);
            System.out.println(dst.get());

            VersionReader vr = file5.versionReader(2);
            ByteBuffer dst2 = ByteBuffer.allocateDirect(10);
            long read2 = vr.read(dst2);
            System.out.println(read2);
            System.out.println(dst2.get());

        } catch (ZboxException e) {
            System.out.println(e);
            return;
        }
    }
}

