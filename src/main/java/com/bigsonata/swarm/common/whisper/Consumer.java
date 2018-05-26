package com.bigsonata.swarm.common.whisper;

public abstract class Consumer<T> implements Disposable, Initializable, MessageHandler<T> {}
