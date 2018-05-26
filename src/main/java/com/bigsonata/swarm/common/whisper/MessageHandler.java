package com.bigsonata.swarm.common.whisper;

public interface MessageHandler<T> {

  /**
   * Consume a message
   *
   * @param topic
   * @param message
   * @throws Exception
   */
  void consume(String topic, T message) throws Exception;
}
