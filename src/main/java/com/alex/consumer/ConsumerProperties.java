package com.alex.consumer;

import java.io.IOException;
import java.util.Properties;

public class ConsumerProperties {

  private static final String PROPERTIES_FILE = "consumer.properties";

  private Properties properties;

  public ConsumerProperties() {
    this.properties = loadConsumerProperties();
  }

  public Properties getProperties() {
    return properties;
  }

  private Properties loadConsumerProperties() {
    Properties props = new Properties();
    try {
      props.load(getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE));
    } catch (IOException e) {
      throw new RuntimeException("Failed to load properties from " + PROPERTIES_FILE, e);
    }

    return props;
  }

  public String getTopic() {
    return properties.getProperty("topic");
  }
}
