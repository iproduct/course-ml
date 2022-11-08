package course.kafka.model;

import lombok.Data;

@Data
public class DoubleStatistics {
   private long count = 0;
   private double sum = 0;
   private double average = 0;
   private double min = Double.MAX_VALUE;
   private double max = Double.MIN_VALUE;

   private long timestamp = 0;

   public DoubleStatistics() {
   }

   public DoubleStatistics(long count, double sum, double average) {
      this.count = count;
      this.sum = sum;
      this.average = average;
   }

   public DoubleStatistics(long count, double sum, double average, double min, double max) {
      this.count = count;
      this.sum = sum;
      this.average = average;
      this.min = min;
      this.max = max;
   }
}
