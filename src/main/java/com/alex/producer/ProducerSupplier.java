package com.alex.producer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ProducerSupplier<K, V> implements Producer<K, V> {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ProducerSupplier.class);

  private ProducerProperties producerProperties;
  private ScheduledExecutorService executorService;
  private AtomicReference<KafkaProducer<K, V>> mainKafkaProducer;
  private KafkaProducer<K, V> backupKafkaProducer;

  public ProducerSupplier(ProducerProperties producerProperties) {
    this.producerProperties = producerProperties;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.mainKafkaProducer = new AtomicReference<>(new KafkaProducer<K, V>(producerProperties.getProperties()));

    executorService.scheduleAtFixedRate(this::rotateProducer, 35, 45, TimeUnit.SECONDS);
  }

  public KafkaProducer<K, V> get() {
    return mainKafkaProducer.get();
  }

  protected void rotateProducer() {
    LOG.info("Rotating producer");
    KafkaProducer<K, V> nextProducer = new KafkaProducer<>(producerProperties.getProperties());
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

  @Override
  public void initTransactions() {
    get().initTransactions();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    get().beginTransaction();
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map, String s) throws ProducerFencedException {
    get().sendOffsetsToTransaction(map, s);
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    get().commitTransaction();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    get().abortTransaction();
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
    return get().send(producerRecord);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
    return get().send(producerRecord, callback);
  }

  @Override
  public void flush() {
    get().flush();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String s) {
    return get().partitionsFor(s);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return get().metrics();
  }

  @Override
  public void close() {
    get().close();
  }

  @Override
  public void close(long l, TimeUnit timeUnit) {
    get().close(l, timeUnit);
  }
}