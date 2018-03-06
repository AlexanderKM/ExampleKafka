package com.alex.producer;

public class ProducerMain {

  public static void main(String[] args) {

    WrappedKafkaProducer wrappedKafkaProducer = new WrappedKafkaProducer();
    // wrappedKafkaProducer.sendMessageBatches(10);
    wrappedKafkaProducer.sendForever();
    //wrappedKafkaProducer.close();
  }
}
