package com.example.kstream;

import static com.example.constants.Constant.TOPIC_GAME;
import static com.example.constants.Constant.TOPIC_GAME_PLAY;
import static com.example.constants.Constant.TOPIC_GAME_PURCHASE;

import com.example.constants.GameAction;
import com.example.message.Game;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafka
@EnableKafkaStreams
@Configuration
public class Stream {

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
        .mapValues((Game::getGameName))
        .to(TOPIC_GAME_PURCHASE);
//    stream.print(Printed.toSysOut());
    return stream;
  }

}
