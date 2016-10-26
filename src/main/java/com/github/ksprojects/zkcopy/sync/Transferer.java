package com.github.ksprojects.zkcopy.sync;

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

  //  Node readRemote(ZooKeeperPath path) {
  //    return read(REMOTE_SERVER, path);
  //  }


  // For testing. We only ever push values.
  void pull(ZooKeeperPath path) {
    try {
      LOG.info(String.format("Pulling: %s", path));
      Node node = readLocal(path);
      if (node == null) {
        throw new SyncException("No node for path: " + path);
      }

      remoteClient.setData().forPath("/settings/test", node.getData());

      verify(node, path);
    } catch (Exception e) {
      throw new SyncException(e);
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


  private void verify(Node fromNode, ZooKeeperPath path) {
    Node updatedNode = read(remoteClient, path);

    if (!Arrays.equals(updatedNode.getData(), fromNode.getData())) {
      throw new SyncException("New value not written to: " + path);
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


  private static String toSource(String server, ZooKeeperPath path) {
    return server + ":2181/warden/" + path.path; // todo Dupe stuff.
  }
}
