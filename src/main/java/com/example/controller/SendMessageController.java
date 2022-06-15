package com.example.controller;

import com.example.producer.Producer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class SendMessageController {

  private Producer producer;

  public SendMessageController(Producer producer) {
    this.producer = producer;
  }

  @GetMapping("send")
  public String postMessage(){
    producer.sendMessageFromCSV();
    System.out.println("HELLO");
    return "MESSAGE SENT";
  }
}
