#!/bin/bash

set -ev

sodium=libsodium-1.0.15
base_dir=$TRAVIS_BUILD_DIR/libsodium
sodium_dir=$base_dir/$sodium

mkdir -p $base_dir

if [ ! -f $sodium_dir/Makefile ]; then
  wget https://download.libsodium.org/libsodium/releases/$sodium.tar.gz
  tar -xzf $sodium.tar.gz -C $base_dir
  cd $sodium_dir && ./configure --prefix=/usr && make
fi

cd $sodium_dir && sudo make install
