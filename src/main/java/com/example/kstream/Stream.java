package com.example.kstream;

import com.example.message.Game;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.support.converter.ByteArrayJsonMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;


@EnableKafka
@EnableKafkaStreams
@Configuration
public class Stream {

//
//  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
//public KafkaStreamsConfiguration kStreamsConfigs() {
//  Map<String, Object> props = new HashMap<>();
//  props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
//  props.put("sasl.mechanism", "PLAIN");
//  props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule   required username='EB5K7URKYLHPXFFB'   password='UemboEDLApv5bW/mOAGWLevR0cEHh/iY4bezM+jcn5EHqQmHpsiCi4mOX+Ph4uDN';");
//  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
//  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092");
//  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//  props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
//  return new KafkaStreamsConfiguration(props);
//}

    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
      return fb -> fb.setStateListener((newState, oldState) -> {
        System.out.println("State transition from " + oldState + " to " + newState);
      });
    }

    @Bean
    public KStream<String, Game> kStream(StreamsBuilder kStreamBuilder) {
      System.out.println("********INSIDE THIS METHOD FIRST******");
      KStream<String, Game> stream = kStreamBuilder.stream("FIRST_6");
      stream
          .filter((key,value) -> "PLAY".equalsIgnoreCase(value.getBehaviour()))
//          .mapValues((ValueMapper<String, Game>) String::toUpperCase)
//          .groupByKey()
//          .windowedBy(TimeWindows.of(Duration.ofMillis(1000)))
//          .reduce((String value1, String value2) -> value1 + value2,
//              Named.as("windowStore"))
//          .toStream()
//          .map((windowedId, value) -> new KeyValue<>(windowedId.key(), value))
//          .filter((i, s) -> s.length() > 40)
          .to("PLAY_6");

//      stream.print(Printed.toSysOut());
//      System.out.println("***********INSIDE PLAY TOPICS");

      return stream;
    }

  @Bean
  public JsonMessageConverter jsonMessageConverter() {
    return new ByteArrayJsonMessageConverter();
  }

  @Bean
  public KStream<String, Game> playStream(StreamsBuilder kStreamBuilder) {
    System.out.println("********INSIDE THIS METHOD SECOND******");
    KStream<String, Game> stream = kStreamBuilder.stream("FIRST_6");
    stream
        .filter((key, value) -> "PURCHASE".equalsIgnoreCase(value.getBehaviour()))
//        .filter(value -> "PURCHASE".equalsIgnoreCase(value.))
        .to("PURCHASE_6");

    System.out.println("*************INSIDE PURCHASE");
//    stream.print(Printed.toSysOut());


    return stream;
  }
}
