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

  private Listener listener;


  Depot(SyncConfig config) {
    this.config = config;
  }


  synchronized Listener getListener() {
    if (listener == null) {
      listener = newListener();
    }

    return listener;
  }

  private Listener newListener() {
    LOG.info("Building local listener...");
    CuratorFramework localClient = newNamespacedClient(config.getLocalHost());
    CuratorFramework remoteClient = newNamespacedClient(config.getRemoteHost());
    Transferer transferer = new Transferer(localClient, remoteClient);

    CuratorFramework client = newClient(config.getLocalHost());
    return new Listener(client, transferer);
  }


  private static CuratorFramework newNamespacedClient(String host) {
    CuratorFramework curator = newClient(host);
    return curator.usingNamespace("warden");
  }

  private static CuratorFramework newClient(String host) {
    LOG.info("Building client with host: {}", host);
    RetryPolicy retryPolicy = (i, l, retrySleeper) -> false;
    CuratorFramework curator = CuratorFrameworkFactory.newClient(host + ":2181", retryPolicy);
    curator.start();
    return curator;
  }
}
