package com.zhiboclub.ycapp.Spark;

import com.alibaba.fastjson.JSON;
import com.zhiboclub.ycapp.Bean.EventsMesgInfo;
import com.zhiboclub.ycapp.DBopts.PGCopyInUtils;
import org.apache.hadoop.mapreduce.jobhistory.Events;
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

import java.sql.ResultSet;
import java.util.*;

public class EventsUserFlowWithSpark {

    public static void main(String[] args) {
        //构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setAppName("YcKafka2SparkUserFlow").setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("/streaming_checkpoint");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "127.0.0.1:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test");
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

            JavaDStream<String> words = stream.flatMap((FlatMapFunction<ConsumerRecord<String, String>, String>) s -> {
                    List<String> list = new ArrayList<>();
                    list.add(s.value());
                    System.out.println(s.value());
                    return list.iterator();
            });
            JavaPairDStream<String, Integer> wordsAndOne = words.mapToPair((PairFunction<String, String, Integer>) word -> {
                //解析kafka中的events数据
                EventsMesgInfo mesgInfo = JSON.parseObject(word, EventsMesgInfo.class);
                String uid = mesgInfo.getUser().getUserId();
                String sql = "select DISTINCT \"userId\",\"anchorId\",\"startTime\" from events where \"userId\"="+uid+" and \"startTime\" < '"+mesgInfo.getStartTime()+"' ORDER BY \"startTime\"";
                ResultSet rs = PGCopyInUtils.getinstance().query(sql);
                List<String> viewanchorId = new ArrayList<>();
               while(rs.next()){
                    viewanchorId.add(rs.getString("anchorId"));
                }
                System.out.println("++++++++++++++++++"+sql);
                System.out.println("UserId:"+uid+";viewlist:"+viewanchorId);

                return new Tuple2<>(mesgInfo.getLiveId(), 1);
            });


            JavaPairDStream<String, Integer> countLivePeoples = words.mapToPair((PairFunction<String, String, Integer>) word -> {
                //解析kafka中的events数据
                EventsMesgInfo mesgInfo = JSON.parseObject(word, EventsMesgInfo.class);
                String uid = mesgInfo.getUser().getUserId();
                return new Tuple2<>(mesgInfo.getLiveId()+"-"+mesgInfo.getStartTime().getTime(), 1);
            });



            //历史累计 60秒checkpoint一次
//            DStream<Tuple2<String, Integer>> result = wordsAndOne.updateStateByKey(((Function2<List<Integer>, org.apache.spark.api.java.Optional<Integer>, org.apache.spark.api.java.Optional<Integer>>) (values, state) -> {
//                Integer updatedValue = 0;
//                if (state.isPresent()) {
//                    updatedValue = Integer.parseInt(state.get().toString());
//                }
//                for (Integer value : values) {
//                    updatedValue += value;
//                }
//                return Optional.of(updatedValue);
//            })).checkpoint(Durations.seconds(60));
//
//            result.print();

            //聚合本次5s的拉取的数据
            JavaPairDStream<String, Integer> wordsCount = wordsAndOne.reduceByKeyAndWindow((Function2<Integer, Integer, Integer>) (a, b) -> a + b ,Durations.seconds(15), Durations.seconds(5));
            wordsCount.print();
//            JavaPairDStream<String, Integer> peopleCount = countLivePeoples.reduceByKeyAndWindow((Function2<Integer, Integer, Integer>) (a, b) -> a + b ,Durations.seconds(15), Durations.seconds(5));
//            peopleCount.print();

            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}