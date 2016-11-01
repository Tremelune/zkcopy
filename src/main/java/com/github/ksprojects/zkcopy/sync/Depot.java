package com.github.ksprojects.zkcopy.sync;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Not quite a factory...not quite a locator...
class Depot {
  private static final Logger LOG = LoggerFactory.getLogger(Depot.class);

  private final SyncConfig config;

  private CuratorFramework localClient;
  private CuratorFramework remoteClient;
  private Listener listener;
  private Transferer transferer;


  Depot(SyncConfig config) {
    this.config = config;
  }


  synchronized Listener getListener() {
    if (listener == null) {
      LOG.info("Building local listener...");
      listener = new Listener(getLocalClient(), getTransferer());
    }

    return listener;
  }


  synchronized Transferer getTransferer() {
    if (transferer == null) {
      LOG.info("Building transferer...");
      transferer = new Transferer(getLocalClient(), getRemoteClient());
    }

    return transferer;
  }


  private synchronized CuratorFramework getLocalClient() {
    if (localClient == null) {
      LOG.info("Building local client...");
      localClient = newClient(config.getLocalHost());
    }

    return localClient;
  }


  private synchronized CuratorFramework getRemoteClient() {
    if (remoteClient == null) {
      LOG.info("Building remote client...");
      remoteClient = newClient(config.getRemoteHost());
    }

    return remoteClient;
  }


  private static CuratorFramework newClient(String host) {
    LOG.info("Building client with host: {}", host);
    RetryPolicy retryPolicy = (i, l, retrySleeper) -> false;
    CuratorFramework curator = CuratorFrameworkFactory.newClient(host + ":2181", retryPolicy);
    curator.start();
    return curator.usingNamespace("warden");
  }
}
