package com.alex.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simulate a fake service that holds a lock
 */
public class LockService {

  private static final Logger LOG = LoggerFactory.getLogger(LockService.class);

  private final ReentrantLock reentrantLock;
  private final ScheduledExecutorService executorService;

  public LockService() {
    this.reentrantLock = new ReentrantLock();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  public void start() {
    this.executorService.scheduleAtFixedRate(this::runOnce, 60, 60, TimeUnit.SECONDS);
  }

  public ReentrantLock getLock() {
    return reentrantLock;
  }

  public void runOnce() {
    LOG.info("LockService trying to lock");
    reentrantLock.lock();
    LOG.info("Lock service got lock-- Sleeping ...");
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      LOG.error("Failed to sleep", e);
    } finally {
      reentrantLock.unlock();
    }
    LOG.info("LockService Unlocked!");
  }
}
