package com.alex.producer;

import com.alex.core.ImmutableTimedMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class WrappedKafkaProducer {

  private static final Logger LOG = LoggerFactory.getLogger(WrappedKafkaProducer.class);
  private static final Long MESSAGES_PER_BATCH = 1000L;
  private static final Long MESSAGE_TIMEOUT = 5000L;

  private final ProducerProperties properties;
  private final KafkaProducer<String, String> producer;
  private final ObjectMapper objectMapper;

  public WrappedKafkaProducer() {
    this.properties = new ProducerProperties();
    this.producer = new KafkaProducer<String, String>(properties.getProperties());
    this.objectMapper = new ObjectMapper();
  }

  public String getTopic() {
    return properties.getTopic();
  }

  public void sendMessageBatches(int messageBatches) {
    for (int batchNumber = 0; batchNumber < messageBatches; batchNumber++) {
      sendMessageBatch();
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
        futures.add(producer.send(new ProducerRecord<>(getTopic(), messageText)));
      }
      futures.forEach(f -> {
        try {
          f.get(MESSAGE_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          LOG.error("Failed to produce message", e);
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  public void close() {
    producer.close(5000, TimeUnit.MILLISECONDS);
  }
}
