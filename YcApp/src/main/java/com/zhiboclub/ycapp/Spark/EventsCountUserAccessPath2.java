package com.zhiboclub.ycapp.Spark;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhiboclub.ycapp.Bean.EventsMesgInfo;
import org.apache.commons.collections.IteratorUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.*;

public class EventsCountUserAccessPath2 {

    private static JavaStreamingContext jssc;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("YcKafka2SparkUserFlow").setMaster("local[10]");

        jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("/streaming_checkpoint");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "127.0.0.1:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test1");
        kafkaParams.put("enable.auto.commit", false);

        // 构建topic set
        String kafkaTopics = "testp3";
        String[] kafkaTopicsSplited = kafkaTopics.split(",");

        Collection<String> topics = new HashSet<>();
        for (String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> event = stream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> record) throws Exception {
                List<String> list = new ArrayList<>();
                EventsMesgInfo mesginfo = JSON.parseObject(record.value(), EventsMesgInfo.class);

                Timestamp timestamp = mesginfo.getStartTime();
                String body = mesginfo.getBody();
                JSONObject jb = JSON.parseObject(body);
                String lid = mesginfo.getLiveId();
                String uid = mesginfo.getUser().getUserId();

                list.add(uid + "@" + record.timestamp() + "##" + lid);
                return list.iterator();
            }
        });
        JavaPairDStream<String, String> userVisit1 = event.mapToPair(e -> new Tuple2<>(e.split("@")[0], e.split("@")[1]));

        JavaPairDStream<String, Iterable<String>> userVisit = userVisit1.groupByKey();

        JavaDStream<String> events = userVisit.map(new Function<Tuple2<String, Iterable<String>>, String>() {
            @Override
            public String call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                StringBuilder value = new StringBuilder();
                String tmp = "";
                List<String> listsort = IteratorUtils.toList(tuple2._2.iterator());
                Collections.sort(listsort);
                for (String i : listsort) {
                    if (tmp.equals(""))
                        tmp = i;
                    else {
                        if (!tmp.equals(i)) {
                            value.append(tuple2._1 + "@" + tmp.split("##")[1] + "@" + i.split("##")[1] + ":@:");
                            tmp = i;
                        }
                    }
                }
                value.append(tuple2._1 + "@" + tmp.split("##")[1]+"@"+tmp.split("##")[1]);
                return value.toString();
            }
        });
        events.print();
        JavaDStream<String> visitline = events.flatMap(line -> Arrays.asList(line.split(":@:")).iterator());
        JavaPairDStream<String, String> totalvisit = visitline.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                if (s.split("@").length == 3)
                    return new Tuple2<>("Total:" + s.split("@")[1] + "@" + s.split("@")[2], "1");
                else
                    return new Tuple2<>(s.split("@")[0], s.split("@")[1]);
            }
        });
//        totalvisit.print();

//        JavaPairDStream<String, Iterable<String>> s = userVisit.updateStateByKey(new Function2<List<Iterable<String>>, Optional<Iterable<String>>, Optional<Iterable<String>>>() {
//            @Override
//            public Optional<Iterable<String>> call(List<Iterable<String>> values, Optional<Iterable<String>> state) throws Exception {
//                Iterable updatedValue;
//                if (state.isPresent()) {
//                    updatedValue = state.get().toString();
//                }
//                for (Iterable value : values) {
//                    updatedValue
//                    updatedValue += value;
//                }
//                return Optional.of(updatedValue);
//            }
//        });
//        s.print();

        JavaPairDStream<String, String> result = totalvisit.reduceByKeyAndWindow((Function2<String, String, String>) (x, y) -> {
                    if (x.contains("##"))
                        return x.split("##")[1]+"@"+y.split("##")[1];
                    else
                        return String.valueOf(Integer.valueOf(x) + Integer.valueOf(y));
                },
                Durations.seconds(600), Durations.seconds(5));

        //统计60s内的时间窗口数据
//        result.foreachRDD((VoidFunction2<JavaPairRDD<String, String>, Time>) (rdd, time) ->
//        {
//            rdd.foreach((VoidFunction<Tuple2<String, String>>) tuple2 -> {
////                if(tuple2._1.contains("Total"))
//                    System.out.println("统计: "+tuple2._1+"->"+tuple2._2);
//            });
//        });
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}

