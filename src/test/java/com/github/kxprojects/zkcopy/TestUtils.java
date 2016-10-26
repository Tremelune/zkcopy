package com.github.kxprojects.zkcopy;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

public class TestUtils {
  public static CuratorFramework localClient() {
    RetryPolicy retryPolicy = (i, l, retrySleeper) -> false;
    CuratorFramework curator = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
    curator.start();
    return curator.usingNamespace("warden");
  }
}
