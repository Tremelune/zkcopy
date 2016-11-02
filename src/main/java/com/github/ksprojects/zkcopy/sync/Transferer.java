package com.github.ksprojects.zkcopy.sync;

import static com.github.ksprojects.zkcopy.sync.ZooKeeperPath.FLAGS;
import static com.github.ksprojects.zkcopy.sync.ZooKeeperPath.SETTINGS;

import java.util.Arrays;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ksprojects.zkcopy.Node;

class Transferer {
  private static final Logger LOG = LoggerFactory.getLogger(Transferer.class);

  private final CuratorFramework localClient;
  private final CuratorFramework remoteClient;


  Transferer(CuratorFramework localClient, CuratorFramework remoteClient) {
    this.localClient = localClient;
    this.remoteClient = remoteClient;
  }


  void push() {
    copyAll(localClient, remoteClient);
  }

  // For testing. We only ever push values.
  void pull() {
    copyAll(remoteClient, localClient);
  }

  // For testing.
  Node readLocal(ZooKeeperPath path) {
    return read(localClient, path);
  }


  private void copyAll(CuratorFramework from, CuratorFramework to) {
    copy(from, to, FLAGS);
    copy(from, to, SETTINGS);
  }


  private void copy(CuratorFramework from, CuratorFramework to, ZooKeeperPath path) {
    try {
      LOG.info(String.format("Copying: %s", path));
      Node root = read(from, path);
      if (root == null) {
        throw new SyncException("No root for path: " + path);
      }

      copy(from, to, root);
    } catch (Exception e) {
      throw new SyncException(e);
    }
  }


  private void copy(CuratorFramework from, CuratorFramework to, Node node) {
    try {
      String absolutePath = getPath(node);
      LOG.info(String.format("Copying: %s", absolutePath));
      remoteClient.setData().forPath(absolutePath, node.getData());

      List<String> children = from.getChildren().forPath(absolutePath);
      for (String childPath : children) {
        if (!"zookeeper".equals(childPath)) { // Reserved
          Node child = new Node(node, childPath);
          node.appendChild(child);
          copy(from, to, child); // Recursion!
          verify(to, node);
        }
      }
    } catch (Exception e) {
      throw new SyncException(e);
    }
  }


  private void verify(CuratorFramework to, Node expected) throws Exception {
    String path = getPath(expected);
    byte[] updatedData = to.getData().forPath(path);

    if (!Arrays.equals(updatedData, expected.getData())) {
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
      String path = getPath(node);
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


  private static String getPath(Node node) {
    return "/" + node.getAbsolutePath();
  }
}
