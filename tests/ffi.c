#include <stdint.h>
#include <stdio.h>

extern uint32_t zbox_init_env();

int main() {
  uint32_t ret = zbox_init_env();
  printf("hello, world. %d\n", ret);
  return ret;
}
