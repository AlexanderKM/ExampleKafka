package com.alex.consumer;

import com.alex.core.ImmutableTimedMessage;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WrappedKafkaConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(WrappedKafkaConsumer.class);

  private ConsumerProperties consumerProperties;
  private KafkaConsumer<String, String> consumer;
  private ObjectMapper objectMapper;
  private MetricRegistry metricRegistry;
  private Histogram messageLagHistogram;
  private Meter messageMeter;
  private ScheduledExecutorService executorService;

  public WrappedKafkaConsumer() {
    this.consumerProperties = new ConsumerProperties();
    this.consumer = new KafkaConsumer<String, String>(consumerProperties.getProperties());
    this.objectMapper = new ObjectMapper();
    this.metricRegistry = new MetricRegistry();
    this.messageLagHistogram = metricRegistry.histogram("message-lag");
    this.messageMeter = metricRegistry.meter("message-count");
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  public void consumeMessages() {
    consumer.subscribe(Collections.singletonList(consumerProperties.getTopic()));
    executorService.scheduleAtFixedRate(new StatsReporterRunnable(), 10000, 60000, TimeUnit.MILLISECONDS);

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(100);

      if (!records.isEmpty()) {
        for (ConsumerRecord<String, String> record : records) {
          String messageJson = record.value();
          try {
            ImmutableTimedMessage timedMessage = objectMapper.readValue(messageJson, new TypeReference<ImmutableTimedMessage>() {});
            messageLagHistogram.update(System.currentTimeMillis() - timedMessage.getTimestamp());
            messageMeter.mark();
          } catch (IOException e) {
            LOG.error("Failed to parse message", e);
          }
        }
        consumer.commitSync();

      }
    }
  }

  private class StatsReporterRunnable implements Runnable {

    public void run() {
      Snapshot snapshot = messageLagHistogram.getSnapshot();
      LOG.info("Message lag 75th: {} ms", snapshot.get75thPercentile());
      LOG.info("Message lag 99th: {} ms", snapshot.get99thPercentile());
      LOG.info("Messages consumed per second: {}", messageMeter.getOneMinuteRate());
    }
  }
}
