package com.github.ksprojects.zkcopy.sync;

// todo Ansiblize this doodad.
public class SyncConfig {
  private String localHost;
  private String remoteHost;


  public String getLocalHost() {
    return localHost;
  }

  public void setLocalHost(String localHost) {
    this.localHost = localHost;
  }

  public String getRemoteHost() {
    return remoteHost;
  }

  public void setRemoteHost(String remoteHost) {
    this.remoteHost = remoteHost;
  }
}
