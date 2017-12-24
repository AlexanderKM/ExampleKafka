package com.alex.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value.Immutable;

@Immutable
@JsonSerialize(as = ImmutableTimedMessage.class)
@JsonDeserialize(as = ImmutableTimedMessage.class)
public abstract class AbstractTimedMessage {

  @JsonProperty("timestamp")
  public abstract long getTimestamp();

  @JsonProperty("text")
  public abstract String getText();
}
