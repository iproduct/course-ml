package course.spring;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class HelloSpark {
    public static void main(String[] args) {
        String inputFileName = "README.md";
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("HelloSparkApp")
                .getOrCreate();
        Dataset<String> data = spark.read().textFile(inputFileName).cache();
        System.out.printf("!!!!! Lines Count: %d\n\n", data.count());
        var sparkLines = data.filter((FilterFunction<String>) s -> s.contains("Spark"));
        long numSparks = sparkLines.count();
        System.out.printf("!!!!! Lines with 'Spark': %d. Filst five lines:\n", numSparks);
        sparkLines.foreach(s -> {
            System.out.println(s);
        });

    }
}
