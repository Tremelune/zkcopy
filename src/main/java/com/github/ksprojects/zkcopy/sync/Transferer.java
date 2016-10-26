package com.github.ksprojects.zkcopy.sync;

import java.util.Arrays;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ksprojects.zkcopy.Node;
import com.github.ksprojects.zkcopy.writer.Writer;

// todo What decides the server to use for this shit? Config? Switch?
public class Transferer {
  private static final Logger LOG = LoggerFactory.getLogger(Transferer.class);
  private static final String LOCAL_SERVER = "localhost";


  void copy(String fromServer, String toServer, ZooKeeperPath path) {
    LOG.info(String.format("Copying %s from %s to %s", path, fromServer, toServer));
    Node node = readLocal(path);
    if (node == null) {
      throw new SyncException("No node on " + fromServer + " for path: " + path);
    }

    String target = toSource(toServer, path);
    Writer writer = new Writer(target, node, false);
    writer.write();

    verify(target, node, path);
  }


  public Node readLocal(ZooKeeperPath path) {
    return read(path);
  }

  //  Node readRemote(ZooKeeperPath path) {
  //    return read(REMOTE_SERVER, path);
  //  }

  private Node read(ZooKeeperPath path) {
    try {
      RetryPolicy retryPolicy = (i, l, retrySleeper) -> false;
      CuratorFramework curator = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
      curator.start();
      CuratorFramework client = curator.usingNamespace("warden");
      Node root = new Node(path.path);
      read(client, root);
      return root;
    } catch (Exception e) {
      throw new SyncException(e);
    }
  }


  private void verify(String toServer, Node fromNode, ZooKeeperPath path) {
    Node updatedNode = read(path);

    if (!Arrays.equals(updatedNode.getData(), fromNode.getData())) {
      throw new SyncException("New value not written to: " + toServer);
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
