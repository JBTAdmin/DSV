package com.example.consumer;

import static com.example.constants.Constant.GROUP_ID;
import static com.example.constants.Constant.TOPIC_GAME;

import com.example.message.Game;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Consumer {
  private final StringBuilder str = new StringBuilder();
  private int i = 0;

  @KafkaListener(topics = TOPIC_GAME, groupId = GROUP_ID)
  public void consume(Game game) {
    str.append(game.getGameName());
    if (i % 50000 == 0) {
      //      log.info("GAMES NAME -> " + str);
    }
    i++;
  }
}
