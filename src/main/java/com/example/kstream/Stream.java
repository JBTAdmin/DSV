package com.example.kstream;

import static com.example.constants.Constant.TOPIC_GAME;
import static com.example.constants.Constant.TOPIC_GAME_PLAY;
import static com.example.constants.Constant.TOPIC_GAME_PURCHASE;

import com.example.constants.GameAction;
import com.example.message.Game;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.annotation.Autowired;
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

  //  private static final String READ_TOPIC = "FIRST_6";
  //
  //  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  // public KafkaStreamsConfiguration kStreamsConfigs() {
  //  Map<String, Object> props = new HashMap<>();
  //  props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
  //  props.put("sasl.mechanism", "PLAIN");
  //  props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule
  // required username='EB5K7URKYLHPXFFB'
  // password='UemboEDLApv5bW/mOAGWLevR0cEHh/iY4bezM+jcn5EHqQmHpsiCi4mOX+Ph4uDN';");
  //  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
  //  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
  // "pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092");
  //  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
  //  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
  // Serdes.String().getClass().getName());
  //  props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
  // WallclockTimestampExtractor.class.getName());
  //  return new KafkaStreamsConfiguration(props);
  // }

//  @Bean
//  public StreamsBuilderFactoryBeanConfigurer configurer() {
//    return fb ->
//        fb.setStateListener(
//            (newState, oldState) -> {
//              System.out.println("State transition from " + oldState + " to " + newState);
//            });
//  }

  @Autowired
  StreamsBuilder kStreamBuilder;

  @Bean
  public KStream<String, Game> playStream(StreamsBuilder kStreamBuilder) {
    KStream<String, Game> stream = kStreamBuilder.stream(TOPIC_GAME);
    stream
        .filter((key, value) -> GameAction.PLAY.name().equalsIgnoreCase(value.getBehaviour()))
        .to(TOPIC_GAME_PLAY);
//    stream.print(Printed.toSysOut());
    return stream;
  }

  @Bean
  public KStream<String, Game> purchaseStream(StreamsBuilder kStreamBuilder) {
    KStream<String, Game> stream = kStreamBuilder.stream(TOPIC_GAME);
    stream
        .filter((key, value) -> GameAction.PURCHASE.name().equalsIgnoreCase(value.getBehaviour()))
        .mapValues((value -> value.getGameName()))
        .to(TOPIC_GAME_PURCHASE);
//    stream.print(Printed.toSysOut());
    return stream;
  }

  private KStream<String, Game> getKStream(){
    return kStreamBuilder.stream(TOPIC_GAME);
  }

}
