package course.spring;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        String inputFileName = "README.md";
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("HelloSparkApp")
                .getOrCreate();
        Dataset<String> data = spark.read().textFile(inputFileName).cache();
        System.out.printf("!!!!! Lines Count: %d\n\n", data.count());
    }
}
