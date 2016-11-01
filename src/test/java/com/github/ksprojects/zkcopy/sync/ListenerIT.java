package com.github.ksprojects.zkcopy.sync;

import static org.testng.Assert.assertEquals;

import java.util.Random;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.testng.annotations.Test;

public class ListenerIT {
  @Test
  public void testListen() throws Exception {
    CuratorFramework localClient = newClient("localhost");
    MockTransferer transferer = new MockTransferer();

    Listener underTest = new Listener(localClient, transferer);
    underTest.listen();
    assertEquals(0, transferer.pushCount);

    int initial = new Random().nextInt();
    byte[] payload = String.valueOf(initial).getBytes();
    localClient.setData().forPath("/warden/settings/test", payload);

    assertEquals(1, transferer.pushCount);
  }


  // This creates a client with no namespace. to Figure out how to do this in production code.
  private static CuratorFramework newClient(String host) {
    RetryPolicy retryPolicy = (i, l, retrySleeper) -> false;
    CuratorFramework curator = CuratorFrameworkFactory.newClient(host + ":2181", retryPolicy);
    curator.start();
    return curator;
  }


  private static class MockTransferer extends Transferer {
    private int pushCount = 0;


    private MockTransferer() {
      super(null, null);
    }


    @Override
    void push() {
      pushCount++;
    }
  }
}
