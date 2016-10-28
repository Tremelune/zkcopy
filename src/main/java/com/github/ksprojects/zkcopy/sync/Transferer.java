package com.github.ksprojects.zkcopy.sync;

import static com.github.ksprojects.zkcopy.sync.ZooKeeperPath.FLAGS;
import static com.github.ksprojects.zkcopy.sync.ZooKeeperPath.SETTINGS;

import java.util.Arrays;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ksprojects.zkcopy.Node;

public class Transferer {
  private static final Logger LOG = LoggerFactory.getLogger(Transferer.class);

  private final CuratorFramework localClient;
  private final CuratorFramework remoteClient;


  Transferer(CuratorFramework localClient, CuratorFramework remoteClient) {
    this.localClient = localClient;
    this.remoteClient = remoteClient;
  }


  public Node readLocal(ZooKeeperPath path) {
    return read(localClient, path);
  }


  void push() {
    copyAll(localClient, remoteClient);
  }

  // For testing. We only ever push values.
  void pull() {
    copyAll(remoteClient, localClient);
  }


  private void copyAll(CuratorFramework from, CuratorFramework to) {
    LOG.info("Copying flags...");
    copy(from, to, FLAGS);
    LOG.info("Copying settings...");
    copy(from, to, SETTINGS);
  }


  private void copy(CuratorFramework from, CuratorFramework to, ZooKeeperPath path) {
    try {
      LOG.info(String.format("Pulling: %s", path));
      Node node = read(from, path);
      if (node == null) {
        throw new SyncException("No node for path: " + path);
      }

      remoteClient.setData().forPath("/settings/test", node.getData());

      verify(to, node, path);
    } catch (Exception e) {
      throw new SyncException(e);
    }
  }


  private void verify(CuratorFramework to, Node expected, ZooKeeperPath path) {
    Node updatedNode = read(to, path);

    if (!Arrays.equals(updatedNode.getData(), expected.getData())) {
      throw new SyncException("New value not written to: " + path);
    }
  }


  private Node read(CuratorFramework client, ZooKeeperPath path) {
    try {
      Node root = new Node(path.path);
      read(client, root);
      return root;
    } catch (Exception e) {
      throw new SyncException(e);
    }
  }


  private void read(CuratorFramework client, Node node) {
    try {
      String path = "/" + node.getAbsolutePath();
      byte[] data = client.getData().forPath(path);
      node.setData(data);

      List<String> children = client.getChildren().forPath(path);
      for (String childPath : children) {
        if (!"zookeeper".equals(childPath)) { // Reserved
          Node child = new Node(node, childPath);
          node.appendChild(child);
          read(client, child); // Recursion!
        }
      }
    } catch (Exception e) {
      throw new SyncException(e);
    }
  }
}
