package com.zhiboclub.ycapp.Spark;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

/**
 * TODO: kafka2streaming  使用的java8的lambda表达式(idea可以alt+enter将方法转换成非lambda表达式的java代码)
 */
public class KafkaStreamingWordCount {
    public static void main(String[] args) {
    	//构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setAppName("YcKafka2Spark").setMaster("local[2]");

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        //checkpoint目录hdfs目录hdfs://localhost:9000/streaming_checkpoint，/streaming_checkpoint则为本地目录
        jssc.checkpoint("/streaming_checkpoint");

        // 构建kafka参数map
        // 主要要放置的是连接的kafka集群的地址（broker集群的地址列表）
        Map<String, Object> kafkaParams = new HashMap<>();
        //Kafka服务监听端口
        kafkaParams.put("bootstrap.servers", "127.0.0.1:9092");
        //指定kafka输出key的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        //指定kafka输出value的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        //消费者ID，随意指定
        kafkaParams.put("group.id", "test");
        //指定从latest(最新,其他版本的是largest这里不行)还是smallest(最早)处开始读取数据
//        kafkaParams.put("auto.offset.reset", "earliest");
        //如果true,consumer定期地往zookeeper写入每个分区的offset
        kafkaParams.put("enable.auto.commit", false);


        // 构建topic set
        String kafkaTopics = "testp3";
        String[] kafkaTopicsSplited = kafkaTopics.split(",");

        Collection<String> topics = new HashSet<>();
        for (String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }


        try {
            // 获取kafka的数据
            final JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            jssc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                    );

            //获取words
            //JavaDStream<String> words = stream.flatMap(s -> Arrays.asList(s.value().split(",")).iterator());
            JavaDStream<String> words = stream.flatMap((FlatMapFunction<ConsumerRecord<String, String>, String>) s -> {
                List<String> list = new ArrayList<>();
                //todo 获取到kafka的每条数据 进行操作
                System.out.print(s.value());
                list.add(s.value() + ",23333");
                return list.iterator();
            });
            //获取word,1格式数据
            JavaPairDStream<String, Integer> wordsAndOne = words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

            //聚合本次5s的拉取的数据
            JavaPairDStream<String, Integer> wordsCount = wordsAndOne.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b);
            wordsCount.print();


            //历史累计 60秒checkpoint一次
            DStream<Tuple2<String, Integer>> result = wordsAndOne.updateStateByKey(((Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (values, state) -> {
                Integer updatedValue = 0;
                if (state.isPresent()) {
                    updatedValue = Integer.parseInt(state.get().toString());
                }
                for (Integer value : values) {
                    updatedValue += value;
                }
                return Optional.of(updatedValue);
            })).checkpoint(Durations.seconds(60));

            result.print();

            //开窗函数 5秒计算一次 计算前15秒的数据聚合
            JavaPairDStream<String, Integer> result2 = wordsAndOne.reduceByKeyAndWindow((Function2<Integer, Integer, Integer>) (x, y) -> x + y,
                    Durations.seconds(15), Durations.seconds(5));
            result2.print();

            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}