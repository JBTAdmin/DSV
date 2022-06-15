package com.example.consumer;

import static com.example.constants.Constant.GROUP_ID;
import static com.example.constants.Constant.TOPIC_GAME;

import com.example.message.Game;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.converter.ByteArrayJsonMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.stereotype.Service;

@Service
public class Consumer {
private StringBuilder str = new StringBuilder();
private int i=0;
//  @Bean
//  public JsonMessageConverter jsonMessageConverter() {
//    return new ByteArrayJsonMessageConverter();
//  }

  @KafkaListener(topics = TOPIC_GAME, groupId = GROUP_ID)
  public void consume(Game game) {
    str.append(game.getGameName());
    if(i >= 200000){
      System.out.println(String.format(" GAME NAME -> %s", game.getGameName()));
    }
    i++;
  }
}
