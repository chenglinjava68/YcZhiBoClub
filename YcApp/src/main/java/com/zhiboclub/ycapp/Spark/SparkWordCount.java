package com.zhiboclub.ycapp.Spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

public class SparkWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[2]");

        @SuppressWarnings("resource")
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("/etc/hosts");

        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) t -> Arrays.asList(t.split(" ")).iterator());
        JavaPairRDD<String, Integer> one =  words.mapToPair((PairFunction<String, String, Integer>) t -> new Tuple2<>(t, 1));

        JavaPairRDD<String, Integer> counts = one.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> (v1 + v2));
        JavaPairRDD<Integer, String> swaped = counts.mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) t -> t.swap());
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        sorted.foreach((VoidFunction<Tuple2<Integer, String>>) t -> System.out.println(t._1 + "------" + t._2 + "times."));
    }
}