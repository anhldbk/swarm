package com.bigsonata.swarm.common.whisper;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class DisruptorBroker<T> extends Broker<T> {
  private static Logger logger = LoggerFactory.getLogger(DisruptorBroker.class.getCanonicalName());
  final Builder<T> builder;
  Disruptor disruptor;
  RingBuffer<Event> ringBuffer;

  public DisruptorBroker(Builder<T> builder) {
    this.builder = builder;
    this.initialize();
  }

  public static Builder newBuilder() {
    return Builder.newInstance();
  }

  public Builder<T> getBuilder() {
    return this.builder;
  }

  ThreadFactory getThreadFactory() {
    return r -> {
      Thread t = new Thread(r);
      t.setDaemon(true);
      return t;
    };
  }

  WorkHandler<Event<T>>[] getWorkersPool() {
    int numWorkers = builder.parallelism;
    WorkHandler<Event<T>>[] workHandlers = new WorkHandler[numWorkers];

    for (int i = 0; i < numWorkers; i++) {
      workHandlers[i] = new DisruptorEventHandler();
    }
    return workHandlers;
  }

  public void produce(String topic, T message) throws Exception {
    if (ringBuffer == null) {
      throw new Exception("Must initialize Disruptor first");
    }
    long sequence = ringBuffer.next(); // Grab the next sequence
    try {
      ringBuffer.get(sequence).setMessage(message).setTopic(topic);

    } finally {
      ringBuffer.publish(sequence);
    }
  }

  @Override
  public void produceAsync(String topic, T message, Consumer<Result> callback) throws Exception {
    produce(topic, message);
    if (callback != null) {
      callback.accept(Result.success());
    }
  }

  public void initialize() {
    logger.info("Initializing...");
    logger.info("> parallelism={}", builder.parallelism);
    logger.info("> lowLatency={}", builder.lowLatency);
    logger.info("> bufferSize={}", builder.bufferSize);

    WaitStrategy waitStrategy = builder.isLowLatency() ? new BusySpinWaitStrategy() : new BlockingWaitStrategy();
    ProducerType producerType =
        builder.getProducerMode() == ProducerMode.SINGLE ? ProducerType.SINGLE : ProducerType.MULTI;
    EventFactory eventFactory = () -> new Event();

    disruptor = new Disruptor(eventFactory, builder.bufferSize, getThreadFactory(), producerType, waitStrategy);
    initializeRingBuffer();

    disruptor.handleEventsWithWorkerPool(getWorkersPool());
    // Start the Disruptor, starts all threads running
    disruptor.start();

    logger.info("Initialized");
  }

  private void initializeRingBuffer() {
    // Get the ring buffer from the Disruptor to be used for publishing.
    ringBuffer = disruptor.getRingBuffer();

    for (int i = 0; i < builder.bufferSize; i++) {
      ringBuffer.get(i).setMessageHandler(builder.messageHandler);
    }
  }

  public void dispose() throws Exception {
    this.dispose(2, TimeUnit.SECONDS);
  }

  @Override
  public void unsubscribe() throws Exception {
    throw new Exception("Unsupported operation");
  }

  public void dispose(long timeout, TimeUnit timeUnit) throws Exception {
    logger.info("Disposing...");
    try {
      disruptor.shutdown(timeout, timeUnit);
    } catch (TimeoutException e) {
      e.printStackTrace();
    } finally {
      logger.info("Disposed");
    }
  }

  public enum ProducerMode {
    SINGLE, MULTIPLE
  }

  private static class DisruptorEventHandler<T> implements WorkHandler<Event<T>> {

    @Override
    public void onEvent(Event<T> event) throws Exception {
      if (event == null) {
        return;
      }
      try {
        event.handle();
      } finally {

      }
    }
  }

  public static class Builder<T> implements BrokerBuilder<T> {
    private boolean lowLatency = false;
    private int bufferSize = 1024;
    private int parallelism = 1;
    private ProducerMode producerMode = ProducerMode.SINGLE;
    private MessageHandler<T> messageHandler = null;

    public static Builder newInstance() {
      return new Builder();
    }

    public ProducerMode getProducerMode() {
      return producerMode;
    }

    public Builder<T> setProducerMode(ProducerMode producerMode) {
      this.producerMode = producerMode;
      return this;
    }

    public MessageHandler<T> getMessageHandler() {
      return messageHandler;
    }

    /**
     * Set the broker's message handler NOTE: The handler must be a pure function which has no side
     * effect
     *
     * @param messageHandler A messageHandler instance
     * @return The current builder
     */
    public Builder<T> setMessageHandler(MessageHandler<T> messageHandler) {
      if (messageHandler != null) {
        this.messageHandler = messageHandler;
      }
      return this;
    }

    public DisruptorBroker<T> build() throws Exception {
      if (messageHandler == null) {
        throw new Exception("Must provide a message handler or a constructor");
      }
      return new DisruptorBroker<>(this);
    }

    public Properties getProperties() throws IllegalArgumentException {
      throw new IllegalArgumentException("Unsupported operation");
    }

    public boolean isLowLatency() {
      return lowLatency;
    }

    public Builder<T> setLowLatency(boolean lowLatency) {
      this.lowLatency = lowLatency;
      return this;
    }

    public int getBufferSize() {
      return bufferSize;
    }

    public Builder<T> setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    /**
     * Set the broker's message handler NOTE: The handler must be a pure function which has no side
     * effect
     *
     * @param messageHandler A message handler
     * @param parallelism    Parallelism
     * @return The current builder
     */
    public Builder<T> setMessageHandler(MessageHandler<T> messageHandler, int parallelism) {
      if (messageHandler != null) {
        this.messageHandler = messageHandler;
      }
      this.setParallelism(parallelism);
      return this;
    }

    public Builder<T> setParallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }
  }
}
