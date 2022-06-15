package algo;

import static com.example.constants.Constant.FILE_LOCATION;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.stream.Stream;

public class SearchWithAlgo {

  public static void main(String[] args) {
    BitSet bit = new BitSet(250000);

    Path filePath = Paths.get(FILE_LOCATION);

    int[] i = {0};
    try (Stream<String> lines = Files.lines(filePath)) {
      lines.forEach(
          t -> {
            int hashCode =
                Hashing.murmur3_128()
                    .newHasher()
                    .putString(t, StandardCharsets.UTF_8)
                    .hash()
                    .asInt();
            if (hashCode < 0) {
              hashCode = Math.abs(hashCode);
            }
            if (bit.get(hashCode)) {
              System.out.println("String is already available in the string" + t);
            } else {
              bit.set(hashCode);
            }
            i[0]++;
          });
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
