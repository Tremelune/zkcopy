package com.github.kxprojects.zkcopy;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

import com.github.ksprojects.zkcopy.sync.SyncConfig;

public class TestUtils {
  public static final SyncConfig CONFIG = config();


  public static CuratorFramework localClient() {
    return newClient(CONFIG.getLocalHost());
  }

  public static CuratorFramework remoteClient() {
    return newClient(CONFIG.getRemoteHost());
  }

  private static CuratorFramework newClient(String host) {
    RetryPolicy retryPolicy = (i, l, retrySleeper) -> false;
    CuratorFramework curator = CuratorFrameworkFactory.newClient(host + ":2181", retryPolicy);
    curator.start();
    return curator.usingNamespace("warden");
  }


  private static SyncConfig config() {
    SyncConfig config = new SyncConfig();
    config.setLocalHost("localhost");
    config.setRemoteHost("ny1-stage-zookeeper001.sta.squarespace.net");
    return config;
  }
}
