package com.example.producer;

import static com.example.constants.Constant.FILE_LOCATION;
import static com.example.constants.Constant.TOPIC_GAME;

import com.example.message.Game;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
@EnableKafka
@Configuration
public class Producer {
  private final KafkaTemplate<String, Game> kafkaTemplate;

  public Producer(KafkaTemplate<String, Game> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @SneakyThrows
  public String sendMessageFromCSV() {

    ClassPathResource cpr = new ClassPathResource(FILE_LOCATION);
    Path filePath = Paths.get(cpr.getURL().getPath());

    int[] i = {0};
    try (Stream<String> lines = Files.lines(filePath)) {
      lines.forEach(
          t -> {
            String[] game = t.split(",");

            kafkaTemplate.send(
                TOPIC_GAME,
                Game.builder()
                    .id(game[0])
                    .gameName(game[1])
                    .behaviour(game[2])
                    .playPurchase(game[3])
                    .build());
            i[0]++;
            if (i[0] % 50000 == 0) {
              log.info("50000 Records sent in Kafka");
            }
          });
    } catch (Exception e) {
      e.printStackTrace();
    }
    return "MESSAGE SENT";
  }
}
