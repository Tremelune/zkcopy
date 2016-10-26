package com.github.ksprojects.zkcopy.sync;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

public class TransfererDepot {
  private Transferer transferer;


  public synchronized Transferer get() {
    if (transferer == null) {
      transferer = create();
    }

    return transferer;
  }


  private Transferer create() {
    // todo How do I Ansiblize this noise?
    CuratorFramework localClient = newClient("localhost");
    CuratorFramework remoteClient = newClient("ny1-stage-zookeeper001.sta.squarespace.net");
    return new Transferer(localClient, remoteClient);
  }

  private static CuratorFramework newClient(String host) {
    RetryPolicy retryPolicy = (i, l, retrySleeper) -> false;
    CuratorFramework curator = CuratorFrameworkFactory.newClient(host + ":2181", retryPolicy);
    curator.start();
    return curator.usingNamespace("warden");
  }
}
