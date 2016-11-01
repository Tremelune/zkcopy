package com.github.ksprojects.zkcopy.sync;

import org.apache.curator.framework.CuratorFramework;

class Listener {
  private CuratorFramework localClient;


  Listener(CuratorFramework localClient) {
    this.localClient = localClient;
  }


  void listen() {
    // todo Now what do we do?
  }
}
