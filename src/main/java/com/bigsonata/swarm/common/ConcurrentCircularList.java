package com.bigsonata.swarm.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

/** @author anhld on 7/28/18 */
public class ConcurrentCircularList<T> implements CircularList<T> {
  private final StampedLock sl = new StampedLock();
  private List<T> items = new ArrayList<>();
  private AtomicInteger size = new AtomicInteger();
  private AtomicInteger index = new AtomicInteger(0);

  /**
   * Get the next item
   *
   * @return If there's no item, returns null. Otherwise, return the next one.
   */
  @Override
  public T next() {
    if (items.isEmpty()) {
      return null;
    }

    return items.get(index.getAndIncrement() % size.get());
  }

  /**
   * Add a new item
   *
   * @param item The item
   * @return Index of the newly added
   */
  @Override
  public int add(final T item) {
    synchronized (this) {
      items.add(item);
    }
    return size.getAndIncrement();
  }

  /**
   * Get the inner buffer's size
   *
   * @return The size
   */
  @Override
  public int size() {
    return size.get();
  }

  /** Reset the internal counter */
  @Override
  public void reset() {
    index.set(0);
  }

  /** Iterate through internal items */
  @Override
  public void forEach(Consumer<T> func) {
    int size = size();
    if (size == 0) {
      return;
    }
    reset();
    for (int i = 0; i < size; i++) {
      func.accept(next());
    }
  }

  @Override
  public void clear() {
    size.set(0);
    synchronized (this) {
      items.clear();
    }
  }
}
