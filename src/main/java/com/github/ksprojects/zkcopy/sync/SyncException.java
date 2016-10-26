package com.github.ksprojects.zkcopy.sync;

class SyncException extends RuntimeException {
  public SyncException(Throwable cause) {
    super(cause);
  }

  public SyncException(String message) {
    super(message);
  }
}
