package com.example.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Game {
  private String id;
  private String gameName;
  private String behaviour;
  private String playPurchase;
}
