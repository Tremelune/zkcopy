package com.github.ksprojects.zkcopy.sync;

import java.util.Arrays;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ksprojects.zkcopy.Node;
import com.github.ksprojects.zkcopy.writer.Writer;

public class Transferer {
  private static final Logger LOG = LoggerFactory.getLogger(Transferer.class);

  private final CuratorFramework localClient;
  private final CuratorFramework remoteClient;


  Transferer(CuratorFramework localClient, CuratorFramework remoteClient) {
    this.localClient = localClient;
    this.remoteClient = remoteClient;
  }


  void copy(String fromServer, String toServer, ZooKeeperPath path) {
    LOG.info(String.format("Copying %s from %s to %s", path, fromServer, toServer));
    Node node = readLocal(path);
    if (node == null) {
      throw new SyncException("No node on " + fromServer + " for path: " + path);
    }

    String target = toSource(toServer, path);
    Writer writer = new Writer(target, node, false);
    writer.write();

    verify(remoteClient, node, path);
  }


  public Node readLocal(ZooKeeperPath path) {
    return read(localClient, path);
  }

  //  Node readRemote(ZooKeeperPath path) {
  //    return read(REMOTE_SERVER, path);
  //  }

  private Node read(CuratorFramework client, ZooKeeperPath path) {
    try {
      Node root = new Node(path.path);
      read(client, root);
      return root;
    } catch (Exception e) {
      throw new SyncException(e);
    }
  }


  private void verify(CuratorFramework client, Node fromNode, ZooKeeperPath path) {
    Node updatedNode = read(client, path);

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
