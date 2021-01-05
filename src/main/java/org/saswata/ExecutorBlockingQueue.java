package org.saswata;

import java.util.concurrent.LinkedBlockingQueue;

public class ExecutorBlockingQueue<T> extends LinkedBlockingQueue<T> {

  public ExecutorBlockingQueue(int size) {
    super(size);
  }

  // ExecutorService calls offer, not put, so "intercept" it here
  @Override
  public boolean offer(T t) {
    try {
      put(t);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    return false;
  }

}
