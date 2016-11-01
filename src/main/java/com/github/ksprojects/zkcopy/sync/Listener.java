package com.github.ksprojects.zkcopy.sync;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// todo We can't use the "warden" namespace for the listner, so...Yikes. Is that okay?
class Listener {
  private static final Logger LOG = LoggerFactory.getLogger(Listener.class);

  private CuratorFramework localClient;
  private Transferer transferer;


  Listener(CuratorFramework localClient, Transferer transferer) {
    this.localClient = localClient;
    this.transferer = transferer;
  }


  void listen() {
    localClient.getCuratorListenable().addListener(this::push);
  }


  private void push(CuratorFramework curatorFramework, CuratorEvent curatorEvent) {
    // Is this...too many events? It should sync anytime anyone touches something.
    LOG.info("Pushing changes...");
    transferer.push();
  }
}
