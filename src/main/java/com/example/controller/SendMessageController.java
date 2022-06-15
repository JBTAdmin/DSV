package com.example.controller;

import com.example.producer.Producer;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class SendMessageController {

  private final Producer producer;

  public SendMessageController(Producer producer) {
    this.producer = producer;
  }

  @GetMapping("send")
  @ResponseStatus(HttpStatus.OK)
  public String postMessage() {
    return producer.sendMessageFromCSV();
  }
}
