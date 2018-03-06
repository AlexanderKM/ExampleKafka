package com.alex.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ProducerSupplier implements Supplier<KafkaProducer<String, String>> {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ProducerSupplier.class);

  private ProducerProperties producerProperties;
  private ScheduledExecutorService executorService;
  private AtomicReference<KafkaProducer<String, String>> mainKafkaProducer;
  private KafkaProducer<String, String> backupKafkaProducer;

  public ProducerSupplier(ProducerProperties producerProperties) {
    this.producerProperties = producerProperties;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.mainKafkaProducer = new AtomicReference<>(new KafkaProducer<String, String>(producerProperties.getProperties()));

    executorService.scheduleAtFixedRate(this::rotateProducer, 35, 45, TimeUnit.SECONDS);
  }

  @Override
  public KafkaProducer<String, String> get() {
    return mainKafkaProducer.get();
  }

  protected void rotateProducer() {
    LOG.info("Rotating producer");
    KafkaProducer<String, String> nextProducer = new KafkaProducer<String, String>(producerProperties.getProperties());
    backupKafkaProducer = mainKafkaProducer.getAndSet(nextProducer);
    try {
      Thread.sleep(2000L);
    } catch (InterruptedException e) {
      LOG.error("Failed to sleep");
    }
    LOG.info("Closing old producer");
    backupKafkaProducer.close();
    LOG.info("Old producer is closed");
  }
}
