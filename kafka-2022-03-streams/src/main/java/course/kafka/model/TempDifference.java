package course.kafka.model;

import lombok.Data;

@Data
public class TempDifference {
   private double value = 0;

   private long timestamp = 0;

   public TempDifference() {
   }

   public TempDifference(double value, long timestamp) {
     this.value = value;
     this.timestamp = timestamp;
   }
}
