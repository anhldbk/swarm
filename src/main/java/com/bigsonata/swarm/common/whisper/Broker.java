package com.bigsonata.swarm.common.whisper;

/** Created by anhld on 1/17/18. */
public abstract class Broker<T> extends Producer<T> implements Initializable, Disposable {
  /**
   * Dispose the consumer interface (if any)
   *
   * @throws Exception
   */
  public abstract void unsubscribe() throws Exception;

  /** Created by anhld on 1/22/18. */
  public interface BrokerBuilder<T> extends Builder<Broker<T>> {}
}
