package com.bigsonata.swarm.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class AtomicCounter {
  private static final int[] FIBONACCI = new int[] {0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89};
  private static Predicate<Integer> POSITIVE_PREDICATE = next -> next > 0;
  private AtomicInteger counter;
  private int index = 0;

  public AtomicCounter() {
    this(0);
  }

  public AtomicCounter(int initial) {
    counter = new AtomicInteger(initial);
  }

  public int get() {
    return counter.get();
  }

  /**
   * Increase the internal value
   *
   * @return The current value
   */
  public int inc() {
    int next = counter.incrementAndGet();
    signal();
    return next;
  }

  /**
   * Decrease the internal value
   *
   * @return The current value
   */
  public int dec() {
    int next = counter.decrementAndGet();
    signal();
    return next;
  }

  /**
   * Decrease the current value only if the validator returns true
   *
   * @param validator A predicate which receives a may-be new value
   * @return The current value
   */
  public int dec(Predicate<Integer> validator) {
    int current, next;
    do {
      current = counter.get();
      if (!validator.test(current)) {
        return current;
      }
      next = current - 1;
    } while (!counter.compareAndSet(current, next));

    signal();
    return next;
  }

  /** Signal that a change is made */
  private void signal() {
    synchronized (this) {
      this.notifyAll();
    }
  }

  /**
   * Decrease the internal value if and only if it's greater than 0
   *
   * @return
   */
  public int decIfPositive() {
    return dec(POSITIVE_PREDICATE);
  }

  public void set(int value) {
    int current, next = value;
    do {
      current = counter.get();
    } while (!counter.compareAndSet(current, next));
    signal();
  }

  /** Wait until counter reaches Zero */
  public void waitUntilZero() {
    waitUntilZero(false);
  }

  /**
   * Wait until the internal counter reaches zero by using adaptive sleep Adaptive sleep is safer
   * than using signal but it may incur high CPU usage
   *
   * @param adaptiveSleep Set this to true to enable adaptive sleep
   */
  public void waitUntilZero(boolean adaptiveSleep) {
    if (!adaptiveSleep) {
      if (counter.get() == 0) {
        return;
      }

      await();
      return;
    }

    await(counter -> counter.get() == 0);
  }

  /** Wait until the signal is set */
  private void await() {
    await(-1);
  }

  private void await(int timeout) {
    synchronized (this) {
      try {
        if (timeout > 0) {
          this.wait(timeout);
          return;
        }
        this.wait();
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * Adaptive wait until a specific condition `f(counter)` is true
   *
   * @param condition The condition
   */
  private void await(Predicate<AtomicInteger> condition) {
    while (!Thread.currentThread().isInterrupted()) {
      if (condition.test(counter)) {
        return;
      }
      index = (index + 1) % FIBONACCI.length;
      int timeout = FIBONACCI[index];
      if (timeout == 0) {
        continue;
      }

      await();
    }
  }

  /**
   * Increase the internal value if and only if its value is lesser than an upper bound Or else, we
   * will wait
   *
   * @param upperBound The upper bound
   * @return The new value (the current)
   */
  public int incIfLessThan(int upperBound) {
    int current, next = 0;
    Predicate<AtomicInteger> condition = counter -> counter.get() < upperBound;
    do {
      await(condition);
      current = counter.get();
      next = current + 1;
      if (counter.compareAndSet(current, next)) {
        break;
      }
    } while (!Thread.currentThread().isInterrupted());

    return next;
  }
}
