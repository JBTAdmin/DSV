package com.example.producer;

import com.example.message.Game;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;

@EnableKafka
@Configuration
public class Producer {
  private final KafkaTemplate<String, Game> kafkaTemplate;

  public Producer(KafkaTemplate<String, Game> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendMessageFromCSV(){

    String path = "/Users/vivekanandgautam/Gautam/Workspace/IdeaWorkspace/KStream-DSV/src/main/resources/data/steam-200k.csv";
    Path filePath = Paths.get(path);

    int[] i = {0};
    try (Stream<String> lines = Files.lines(filePath)) {
      lines.forEach(t -> {
        String[] game = t.split(",");

        kafkaTemplate.send("FIRST_6", Game.builder().id(game[0]).gameName(game[1]).behaviour(game[2]).playPurchase(game[3]).build());
        i[0]++;
        if(i[0] % 10000 ==0){
          System.out.println("100 Records sent in Kafka");
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
