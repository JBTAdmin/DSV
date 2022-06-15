package com.example.consumer;

import static com.example.constants.Constant.GROUP_ID;
import static com.example.constants.Constant.TOPIC_GAME;

import com.example.message.Game;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer {
  private final StringBuilder str = new StringBuilder();
  private int i = 0;

  @KafkaListener(topics = TOPIC_GAME, groupId = GROUP_ID)
  public void consume(Game game) {
    str.append(game.getGameName());
    if (i >= 200000) {
      System.out.println("GAMES NAME -> " + str);
    }
    i++;
  }
}
