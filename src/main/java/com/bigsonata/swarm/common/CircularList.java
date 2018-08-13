package com.bigsonata.swarm.common;

import java.util.function.Consumer;

/** @author anhld on 7/28/18 */
public interface CircularList<T> {

	/**
	 * Get the next item
	 * @return If there's no item, returns null. Otherwise, return the next one.
	 */
    T next();

	/**
	 * Reset the internal counter
	 */
	void reset();

    /**
     * Add a new item
     *
     * @param item The item
     * @return Index of the newly added
     */
    int add(final T item);

	/**
	 * Get the inner buffer's size
	 * @return The size
	 */
	int size();

	void forEach(Consumer<T> func);

	void clear();
}
