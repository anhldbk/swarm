package com.bigsonata.swarm.common.whisper;

public abstract class Producer<T> implements Disposable, Initializable {
  /**
   * Send message synchronously
   *
   * @param topic Topic
   * @param message Message
   * @throws Exception
   */
  public abstract void produce(String topic, T message) throws Exception;

  /**
   * Send message synchronously (if supported) NOTE: topic is ignored and setMessage to null
   *
   * @param message A message
   * @throws Exception
   */
  public void produce(T message) throws Exception {
    produce(null, message);
  }

  /**
   * Send messages asynchronously
   *
   * @param topic Topic
   * @param message Message
   * @param callback A callback to consume result
   * @throws Exception
   */
  public abstract void produceAsync(String topic, T message, Callback callback) throws Exception;

  /**
   * Send messages asynchronously
   *
   * @param topic Topic
   * @param message A message
   * @throws Exception
   */
  public void produceAsync(String topic, T message) throws Exception {
    produceAsync(topic, message, null);
  }

  /**
   * Send message asynchronously (if supported) NOTE: topic is ignored and setMessage to null
   *
   * @param message A message
   * @param callback A callback to consume result
   * @throws Exception
   */
  public void produceAsync(T message, Callback callback) throws Exception {
    produceAsync(null, message, callback);
  }

  /**
   * Send message asynchronously (if supported)
   *
   * @param message A message
   * @throws Exception
   */
  public void produceAsync(T message) throws Exception {
    produceAsync(null, message, null);
  }
}
