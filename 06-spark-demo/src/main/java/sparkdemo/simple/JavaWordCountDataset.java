/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sparkdemo.simple;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCountDataset {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

//    if (args.length < 1) {
//      System.err.println("Usage: JavaWordCount <file>");
//      System.exit(1);
//    }

        SparkSession spark = SparkSession
                .builder()
                .master("local")
//                .master("spark://10.108.6.196:7077")
                .appName("JavaWordCount")
                .getOrCreate();

        Dataset<String> lines = spark.read()
                .textFile("D:\\CourseDML\\spark-3.0.1-bin-hadoop3.2\\README.md");
//                .as(Encoders.bean(String.class));
        ;

        Dataset<String> words = lines
                .flatMap((String s) -> Arrays.asList(s.split("\\W+")).iterator(), Encoders.STRING())
                .map((MapFunction<String, String>) (String w) -> w.toLowerCase(), Encoders.STRING())
                .filter((String word) -> word.length() > 3 && Pattern.matches(".*[A-Za-z]+.*", word));


        Dataset<Tuple2<String, Integer>> ones = words
                .map((MapFunction<String, Tuple2<String, Integer>>)  s ->
                        new Tuple2<>(s, 1), Encoders.tuple(Encoders.STRING(), Encoders.INT()));

        words.groupBy("value").count().sort(org.apache.spark.sql.functions.col("count").desc()).show();

        spark.stop();
    }
}
