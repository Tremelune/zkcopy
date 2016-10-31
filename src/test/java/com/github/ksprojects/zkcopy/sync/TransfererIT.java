package com.github.ksprojects.zkcopy.sync;

import static com.github.ksprojects.zkcopy.sync.ZooKeeperPath.SETTINGS;
import static com.github.kxprojects.zkcopy.TestUtils.localClient;
import static com.github.kxprojects.zkcopy.TestUtils.remoteClient;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.testng.annotations.Test;

import com.github.ksprojects.zkcopy.Node;

public class TransfererIT {
  @Test
  public void testPull() throws Exception {
    // Set a random local value, then overwrite it with whatever is on staging. This avoids messing with staging.
    int initial = new Random().nextInt();
    CuratorFramework localClient = localClient();
    byte[] payload = String.valueOf(initial).getBytes();

    // Set the value, or create it if it doesn't exist yet...
    if (localClient.checkExists().forPath("/settings/test") == null) {
      localClient.create().forPath("/settings/test", payload);
    } else {
      localClient.setData().forPath("/settings/test", payload);
    }

    // Make sure staging has this setting...
    CuratorFramework remoteClient = remoteClient();
    if (remoteClient.checkExists().forPath("/settings/test") == null) {
      remoteClient.create().forPath("/settings/test", "testvalue".getBytes()); // Anything is fine
    }

    byte[] initialBytes = localClient.getData().forPath("/settings/test");
    assertEquals(String.valueOf(initial), new String(initialBytes));

    Transferer underTest = new Transferer(localClient(), remoteClient());
    underTest.pull();

    Node updatedNode = underTest.readLocal(SETTINGS);
    assertNotEquals(new String(updatedNode.getData()), String.valueOf(initial));
  }


  @Test
  public void testRead() throws Exception {
    int random = new Random().nextInt();
    CuratorFramework curator = localClient();
    byte[] payload = String.valueOf(random).getBytes();
    curator.setData().forPath("/settings/test", payload);

    Transferer underTest = new Transferer(localClient(), null);
    Node node = underTest.readLocal(SETTINGS);
    Node readSetting = null;
    for (Node child : node.getChildren()) {
      if (child.getPath().equals("test")) {
        readSetting = child;
      }
    }

    if (readSetting == null) {
      fail();
    } else {
      String readValue = new String(readSetting.getData());
      assertEquals(readValue, String.valueOf(random));
    }
  }
}
