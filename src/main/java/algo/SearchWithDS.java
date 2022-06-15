package algo;

import static com.example.constants.Constant.FILE_LOCATION;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class SearchWithDS {

  public static void main(String[] args) {

    BloomFilter<String> filter =
        BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), 300000, 0.01);

    Path filePath = Paths.get(FILE_LOCATION);

    int[] i = {0};
    try (Stream<String> lines = Files.lines(filePath)) {
      lines.forEach(
          t -> {
            if (!filter.mightContain(t)) {
              filter.put(t);
            } else {
              System.out.println("Given Object is already in list-" + t + "-@-" + i[0]);
            }
            i[0]++;
          });
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
