package com.github.ksprojects.zkcopy.sync;

import static com.github.ksprojects.zkcopy.sync.ZooKeeperPath.SETTINGS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.testng.annotations.Test;

import com.github.ksprojects.zkcopy.Node;
import com.github.kxprojects.zkcopy.TestUtils;

public class TransfererIT {
  @Test
  public void testRead() throws Exception {
    int random = new Random().nextInt();
    CuratorFramework curator = TestUtils.localClient();
    byte[] payload = String.valueOf(random).getBytes();
    curator.setData().forPath("/settings/test", payload);

    Transferer underTest = new Transferer();
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
