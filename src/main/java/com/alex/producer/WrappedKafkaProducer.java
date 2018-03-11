package com.alex.producer;

import com.alex.core.ImmutableTimedMessage;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WrappedKafkaProducer {

  private static final Logger LOG = LoggerFactory.getLogger(WrappedKafkaProducer.class);
  private static final Long MESSAGES_PER_BATCH = 200L;
  private static final Long MESSAGE_TIMEOUT = 10000L;

  private final ProducerProperties properties;
  private final ObjectMapper objectMapper;
  private final ScheduledExecutorService executorService;
  private final Histogram produceTimeHistogram;
  private final Meter messagesMeter;
  private final Producer<String, String> delegate;

  public WrappedKafkaProducer() {
    this.properties = new ProducerProperties();
    this.objectMapper = new ObjectMapper();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.produceTimeHistogram = new MetricRegistry().histogram("produce-time");
    this.messagesMeter = new MetricRegistry().meter("messages-produced");
    this.delegate = new ProducerSupplier<>(properties);
  }

  public String getTopic() {
    return properties.getTopic();
  }

  public void sendForever() {
    executorService.scheduleAtFixedRate(this::reportMetrics, 30, 60, TimeUnit.SECONDS);
    while (true) {
      sendMessageBatch();
      LOG.info("Sent {} messages", MESSAGES_PER_BATCH);
      try {
        Thread.sleep(3000L);
      } catch (InterruptedException e) {
        LOG.error("Failed to sleep", e);
      }
    }
  }

  public void sendMessageBatches(int messageBatches) {
    for (int batchNumber = 0; batchNumber < messageBatches; batchNumber++) {
      sendMessageBatch();
      try {
        Thread.sleep(3000L);
      } catch (InterruptedException e) {
        LOG.error("Failed to sleep", e);
      }
    }
    LOG.info("Sent {} message batches with {} messages per batch", messageBatches, MESSAGES_PER_BATCH);
  }

  private void sendMessageBatch() {
    try {
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < MESSAGES_PER_BATCH; i++) {

        long currentTime = System.currentTimeMillis();
        ImmutableTimedMessage message = ImmutableTimedMessage.builder()
            .timestamp(currentTime)
            .text("Some message text")
            .build();
        String messageText = objectMapper.writeValueAsString(message);
        futures.add(delegate.send(new ProducerRecord<>(getTopic(), messageText)));
      }
      futures.forEach(f -> {
        try {
          long startTime = System.currentTimeMillis();
          f.get(MESSAGE_TIMEOUT, TimeUnit.MILLISECONDS);
          produceTimeHistogram.update(System.currentTimeMillis() - startTime);
          messagesMeter.mark();
        } catch (Exception e) {
          LOG.error("Failed to produce message", e);
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  protected void reportMetrics() {
    Snapshot snapshot = produceTimeHistogram.getSnapshot();

    /*
    StringBuilder sb = new StringBuilder();
    Map<MetricName, ? extends Metric> metrics = delegate.metrics();
    for (MetricName metricName : metrics.keySet()) {
      sb.append("\t");
      sb.append(metricName.name());
      sb.append(": ");
      sb.append(metrics.get(metricName).metricValue());
      sb.append("\n");
    }
    LOG.info("All metrics:\n {}", sb);
    */
    LOG.info("Local produce time 75th: {}", snapshot.get75thPercentile());
    LOG.info("Local produce time 99th: {}", snapshot.get99thPercentile());
    LOG.info("Messages produced per second: {}", messagesMeter.getOneMinuteRate());
  }

  public void close() {
    delegate.close(5000, TimeUnit.MILLISECONDS);
  }
}
