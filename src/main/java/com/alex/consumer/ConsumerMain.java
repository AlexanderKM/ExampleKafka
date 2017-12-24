package com.alex.consumer;

public class ConsumerMain {

  public static void main(String[] args) {

    WrappedKafkaConsumer wrappedKafkaConsumer = new WrappedKafkaConsumer();
    wrappedKafkaConsumer.consumeMessages();
  }
}
