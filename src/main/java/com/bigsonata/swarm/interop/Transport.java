package com.bigsonata.swarm.interop;

import java.io.IOException;

public abstract class Transport {
  /**
   * send message to master
   *
   * @param message A Message
   * @throws IOException IOException
   */
  public abstract void send(Message message) throws Exception;

  public abstract void onMessage(Message message) throws Exception;

  public abstract void initialize() throws Exception;

  public abstract void dispose();

  public abstract void onConnected();

  public abstract void onDisconnected();

  public enum State {
    CONNECTED,
    DISCONNECTED,
    CLOSING,
    CLOSED
  }
}
