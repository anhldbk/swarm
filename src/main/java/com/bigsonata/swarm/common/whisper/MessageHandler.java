package com.bigsonata.swarm.common.whisper;

public interface MessageHandler<T> {

  /**
   * Consume a message
   *
   * @param topic Topic to consume from
   * @param message Message
   * @throws Exception Exception
   */
  void consume(String topic, T message) throws Exception;
}
