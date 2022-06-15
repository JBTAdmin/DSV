package com.example.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer {

  @KafkaListener(topics = "FIRST_6", groupId = "group_id")
  public void consume(String game) {
    System.out.println(String.format(" GAME -> %s", game));
  }
}
