package com.alex.producer;

import java.io.IOException;
import java.util.Properties;

public class ProducerProperties {

  private static final String PROPERTIES_FILE = "producer.properties";

  private Properties properties;

  public ProducerProperties() {
    this.properties = loadProducerProperties();
  }

  public Properties getProperties() {
    return properties;
  }

  public String getTopic() {
    return properties.getProperty("topic");
  }

  private Properties loadProducerProperties() {
    Properties props = new Properties();
    try {
      props.load(getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE));
    } catch (IOException e) {
      throw new RuntimeException("Failed to load properties from " + PROPERTIES_FILE, e);
    }

    return props;
  }
}
