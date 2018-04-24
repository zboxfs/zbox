#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "zbox.h"

int test_repo() {
    const char *uri = "mem://repo";
    const char *pwd = "pwd";
    bool result;
    int ret;

    ret = zbox_init_env();
    assert(!ret);

    // opener
    zbox_opener opener = zbox_create_opener();
    zbox_opener_ops_limit(opener, ZBOX_OPS_MODERATE);
    zbox_opener_mem_limit(opener, ZBOX_MEM_INTERACTIVE);
    zbox_opener_cipher(opener, ZBOX_CIPHER_XCHACHA);
    zbox_opener_create(opener, true);
    zbox_opener_version_limit(opener, 2);

    // open repo
    zbox_repo repo;
    ret = zbox_open_repo(&repo, opener, uri, pwd);
    assert(!ret);
    zbox_free_opener(opener);

    // repo exists
    ret = zbox_repo_exists(&result, "wrong uri");
    assert(ret == ZBOX_ERR_INVALIDURI);  // InvalidUri error

    // repo info
    struct zbox_repo_info info;
    zbox_get_repo_info(&info, repo);
    assert(info.version_limit == 2);
    assert(!strcmp(info.uri, uri));
    assert(info.ops_limit == ZBOX_OPS_MODERATE);
    assert(info.mem_limit == ZBOX_MEM_INTERACTIVE);
    assert(info.cipher == ZBOX_CIPHER_XCHACHA);
    assert(!info.is_read_only);
    assert(info.created > 0);
    zbox_destroy_repo_info(&info);

    // reset password
    ret = zbox_repo_reset_password(repo, pwd, "new pwd",
        ZBOX_OPS_INTERACTIVE, ZBOX_MEM_MODERATE);

    // path exists
    result = zbox_repo_path_exists(repo, "/");
    assert(result);
    result = zbox_repo_path_exists(repo, "/non-exists");
    assert(!result);

    // is file and is dir
    result = zbox_repo_is_file(repo, "/");
    assert(!result);
    result = zbox_repo_is_dir(repo, "/");
    assert(result);

    // create file
    zbox_file file;
    ret = zbox_repo_create_file(&file, repo, "/file");
    assert(!ret);
    zbox_close_file(file);

    // open and close file
    zbox_file file2;
    ret = zbox_repo_open_file(&file2, repo, "/file");
    assert(!ret);
    zbox_close_file(file2);

    // create dir
    ret = zbox_repo_create_dir(repo, "/dir");
    assert(!ret);
    ret = zbox_repo_create_dir_all(repo, "/dir1/dir2/dir3");
    assert(!ret);

    // read dir
    struct zbox_dir_entry_list dlist;
    ret = zbox_repo_read_dir(&dlist, repo, "/");
    assert(!ret);
    assert(dlist.entries);
    assert(dlist.len == 3);
    assert(!strcmp(dlist.entries[0].path, "/file"));
    assert(!strcmp(dlist.entries[0].file_name, "file"));
    assert(!strcmp(dlist.entries[1].path, "/dir"));
    assert(!strcmp(dlist.entries[1].file_name, "dir"));
    assert(!strcmp(dlist.entries[2].path, "/dir1"));
    assert(!strcmp(dlist.entries[2].file_name, "dir1"));
    zbox_destroy_dir_entry_list(&dlist);

    // metadata
    struct zbox_metadata meta;
    ret = zbox_repo_metadata(&meta, repo, "/dir");
    assert(!ret);
    assert(meta.ftype == ZBOX_FTYPE_DIR);

    // history
    struct zbox_version_list vlist;
    ret = zbox_repo_history(&vlist, repo, "/file");
    assert(!ret);
    assert(vlist.len == 1);
    assert(vlist.versions[0].num == 1);
    assert(vlist.versions[0].len == 0);
    zbox_destroy_version_list(&vlist);

    // copy
    ret = zbox_repo_copy("/file2", "/file", repo);
    assert(!ret);

    // remove file
    ret = zbox_repo_remove_file("/file2", repo);
    assert(!ret);

    // remove dir
    ret = zbox_repo_remove_dir("/dir", repo);
    assert(!ret);
    ret = zbox_repo_remove_dir_all("/dir1", repo);
    assert(!ret);

    // rename
    ret = zbox_repo_rename("/file3", "/file", repo);
    assert(!ret);

    zbox_close_repo(repo);

    return ret < 0 ? ret : 0;
}

int test_file() {
    const char *uri = "mem://repo2";
    const char *pwd = "pwd";
    int ret;

    ret = zbox_init_env();
    assert(!ret);

    // opener
    zbox_opener opener = zbox_create_opener();
    zbox_opener_create(opener, true);

    // open repo
    zbox_repo repo;
    ret = zbox_open_repo(&repo, opener, uri, pwd);
    assert(!ret);
    zbox_free_opener(opener);

    // create file
    zbox_file file;
    ret = zbox_repo_create_file(&file, repo, "/file");
    assert(!ret);

    // metadata
    struct zbox_metadata meta;
    ret = zbox_file_metadata(&meta, file);
    assert(!ret);
    assert(meta.ftype == ZBOX_FTYPE_FILE);
    assert(meta.len == 0);

    // history
    struct zbox_version_list vlist;
    ret = zbox_file_history(&vlist, file);
    assert(!ret);
    assert(vlist.len == 1);
    assert(vlist.versions[0].num == 1);
    assert(vlist.versions[0].len == 0);
    zbox_destroy_version_list(&vlist);

    // current version
    size_t ver;
    ret = zbox_file_curr_version(&ver, file);
    assert(!ret);
    assert(ver == 1);

    // write and finish
    uint8_t buf[3] = { 1, 2, 3 };
    ret = zbox_file_write(file, buf, 3);
    assert(ret == 3);
    ret = zbox_file_finish(file);
    assert(!ret);
    ret = zbox_file_history(&vlist, file);
    assert(!ret);
    assert(vlist.len == 2);
    zbox_destroy_version_list(&vlist);

    // read
    uint8_t dst[3] = { 0 };
    ret = zbox_file_read(dst, 3, file);
    assert(ret == 0);
    ret = zbox_file_seek(file, 0, SEEK_SET);
    assert(ret == 0);
    ret = zbox_file_read(dst, 3, file);
    assert(ret == 3);
    assert(!memcmp(dst, buf, 3));

    // write once
    buf[0] = 4; buf[1] = 5; buf[2] = 6;
    assert(zbox_file_seek(file, 0, SEEK_SET) == 0);
    ret = zbox_file_write_once(file, buf, 3);
    assert(!ret);

    // seek
    ret = zbox_file_seek(file, 1, SEEK_SET);
    assert(!ret);

    // version reader
    zbox_version_reader rdr;
    ret = zbox_file_version_reader(&rdr, 3, file);
    assert(!ret);
    ret = zbox_file_version_read(dst, 3, rdr);
    assert(ret == 3);
    assert(!memcmp(dst, buf, 3));

    // version reader seek
    ret = zbox_file_version_reader_seek(rdr, 1, SEEK_SET);
    assert(!ret);
    ret = zbox_file_version_read(dst, 2, rdr);
    assert(ret == 2);
    assert(!memcmp(dst, &buf[1], 2));
    zbox_close_version_reader(rdr);

    // set length
    ret = zbox_file_set_len(file, 2);
    assert(!ret);
    ret = zbox_file_metadata(&meta, file);
    assert(!ret);
    assert(meta.len == 2);

    zbox_close_file(file);
    zbox_close_repo(repo);

    return ret < 0 ? ret : 0;
}

int main() {
    int ret;

    ret = test_repo();
    if (ret) {
        return ret;
    }

    ret = test_file();
    return ret;
}
