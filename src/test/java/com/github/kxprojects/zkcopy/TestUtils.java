package com.github.kxprojects.zkcopy;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

public class TestUtils {
  public static CuratorFramework localClient() {
    return newClient("ny1-stage-zookeeper001.sta.squarespace.net");
  }

  public static CuratorFramework remoteClient() {
    return newClient("localhost");
  }

  private static CuratorFramework newClient(String host) {
    RetryPolicy retryPolicy = (i, l, retrySleeper) -> false;
    CuratorFramework curator = CuratorFrameworkFactory.newClient(host + ":2181", retryPolicy);
    curator.start();
    return curator.usingNamespace("warden");
  }
}
