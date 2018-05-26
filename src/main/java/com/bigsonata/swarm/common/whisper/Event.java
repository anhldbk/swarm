package com.bigsonata.swarm.common.whisper;

public class Event<T> {
  protected MessageHandler<T> messageHandler;
  private T message;
  private String topic = null;

  public Event<T> setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  public Event<T> setMessageHandler(MessageHandler<T> messageHandler) {
    this.messageHandler = messageHandler;
    return this;
  }

  public final void handle() throws Exception {
    this.messageHandler.consume(topic, message);
  }

  public T getMessage() {
    return message;
  }

  public Event<T> setMessage(T message) {
    this.message = message;
    return this;
  }
}
