package com.github.ksprojects.zkcopy.sync;

enum ZooKeeperPath {
  FLAGS("flags"),
  SETTINGS("settings");

  final String path;

  ZooKeeperPath(String path) {
    this.path = path;
  }
}
