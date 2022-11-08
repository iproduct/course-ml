package course.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CountSumAvg {
   private long count = 0;
   private double sum = 0;
   private double average = 0;
   private long timestamp = 0;

   public CountSumAvg(long count, double sum, double average) {
      this.count = count;
      this.sum = sum;
      this.average = average;
   }
}
