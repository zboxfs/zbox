#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "zbox.h"

int main() {
  int ret;

  ret = zbox_init_env();
  if (ret) {
      return ret;
  }

  // opener
  zbox_opener opener = zbox_create_opener();
  zbox_opener_create(opener, true);
  zbox_opener_version_limit(opener, 2);

  // repo
  zbox_repo repo;
  ret = zbox_open_repo(&repo, opener, "mem://repo", "pwd");
  zbox_free_opener(opener);
  if (ret) {
      return ret;
  }

  // repo.exists
  bool result;
  ret = zbox_repo_exists(&result, "wrong uri");
  assert(ret == 1020);  // InvalidUri error

  // repo info
  struct zbox_repo_info info;
  zbox_get_repo_info(&info, repo);
  assert(info.version_limit == 2);
  assert(strcmp(info.uri, "mem://repo") == 0);
  assert(!info.is_read_only);
  assert(info.created > 0);
  zbox_destroy_repo_info(&info);

  zbox_close_repo(repo);

  return 0;
}
