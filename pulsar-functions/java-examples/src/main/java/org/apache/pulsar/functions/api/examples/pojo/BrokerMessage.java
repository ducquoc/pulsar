package org.apache.pulsar.functions.api.examples.pojo;

import lombok.*;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * @author ducquoc
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class BrokerMessage<T> implements Serializable {

  private String eventAction;
  private String simpleName;
  private String jsonPayload;
  private T payload;

  public static void main(String[] args) throws Exception {

    final String SERVICE_URL = "pulsar://localhost:6650";
    final String TOPIC_NAME = "test-topic";
    PulsarClient client = PulsarClient.builder().serviceUrl(SERVICE_URL).build();

    Producer<BrokerMessage> producer = client.newProducer(JSONSchema.of(BrokerMessage.class))
        .topic(TOPIC_NAME).create();

    Tick tick = new Tick();
    tick.setStockSymbol("AAPL");
    BrokerMessage brokerMessage = BrokerMessage.builder()
        .eventAction("save").simpleName(Tick.class.getSimpleName()).payload(tick).build();

    Message<BrokerMessage> msg = MessageBuilder.create(JSONSchema.of(BrokerMessage.class))
        .setValue(brokerMessage)
        .build();
    MessageId msgId = producer.send(msg.getValue());
    System.out.println("Published message '" + msg + "' with the ID " + msgId);
    System.exit(0);
  }

}
