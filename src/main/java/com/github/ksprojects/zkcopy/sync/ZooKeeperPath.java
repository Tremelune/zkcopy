package com.github.ksprojects.zkcopy.sync;

public enum ZooKeeperPath {
  FLAGS("flags"),
  SETTINGS("settings"),
  TEST("test"); // Only used for testing.

  final String path;

  ZooKeeperPath(String path) {
    this.path = path;
  }
}
