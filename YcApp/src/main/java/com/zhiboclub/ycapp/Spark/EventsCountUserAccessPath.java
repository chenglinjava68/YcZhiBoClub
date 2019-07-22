package com.zhiboclub.ycapp.Spark;

import com.alibaba.fastjson.JSON;
import com.zhiboclub.ycapp.Bean.EventsMesgInfo;
import com.zhiboclub.ycapp.DBopts.PGCopyInUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
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
import java.text.SimpleDateFormat;
import java.util.*;

public class EventsCountUserAccessPath {

    private static JavaStreamingContext jssc;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("YcKafka2SparkUserFlow").setMaster("local[4]");

        jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("/streaming_checkpoint");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "127.0.0.1:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "consumer");
        kafkaParams.put("enable.auto.commit", false);

        // 构建topic set
        String kafkaTopics = "events-p3";
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
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(ConsumerRecord<String, String> record) throws Exception {
                List<String> list = new ArrayList<>();
                EventsMesgInfo mesginfo = JSON.parseObject(record.value(), EventsMesgInfo.class);

                Timestamp timestamp = mesginfo.getStartTime();
                String lid = mesginfo.getLiveId();
                String uid = mesginfo.getUser().getUserId();

                list.add(uid + "@" + timestamp.getTime() + ":" + lid);
                return list.iterator();
            }
        });

        JavaPairDStream<String, String> userVisit = event.mapToPair(e -> new Tuple2<>(e.split("@")[0], e.split("@")[1]));


//        窗口统计
//        JavaPairDStream<String, String> user = userVisit.reduceByKeyAndWindow((Function2<String, String, String>) (x, y) ->
//            (x+"#"+y)
//        ,Durations.seconds(600), Durations.seconds(5));


//      历史统计
        JavaPairDStream<String, String> user = userVisit.updateStateByKey(new Function2<List<String>, Optional<String>, Optional<String>>() {
            @Override
            public Optional<String> call(List<String> v1, Optional<String> v2) throws Exception {
                String updatedValue = "";
                if (v2.isPresent()) {
                    updatedValue = v2.get();
                }
                for (String value : v1) {
                    if (updatedValue.equals(""))
                        updatedValue = value;
                    else
                        updatedValue += "#" + value;
                }
                return Optional.of(updatedValue);
            }
        });


        user.print(100);

        JavaDStream<String> line = user.map(new Function<Tuple2<String, String>, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Tuple2<String, String> arg) {
                return arg._2;
            }
        });

        JavaDStream<String> words = line.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String s) throws Exception {
                HashSet<String> set = new HashSet<>();
                List<String> data = new ArrayList<>();
                String tmp = "";
                for (String e : s.split("#")) {
                    set.add(e);
                }
                List<String> tempList = new ArrayList<>(set);
                Collections.sort(tempList);
                for (String t : tempList) {
                    if (tmp.equals("")) {
                        tmp = t.split(":")[1];
                    } else {
                        if (!tmp.equals(t.split(":")[1])) {
                            data.add(tmp + "@" + t.split(":")[1]);
                            tmp = t.split(":")[1];
                        }
                    }
                }
                return data.iterator();
            }
        });

//        words.print();

        JavaPairDStream<String, Integer> wordsAndOne = words.mapToPair(e -> new Tuple2<>(e, 1));
        JavaPairDStream<String, Integer> userCount = wordsAndOne.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

        userCount.foreachRDD((VoidFunction2<JavaPairRDD<String, Integer>, Time>) (rdd, time) -> {
            rdd.foreach((VoidFunction<Tuple2<String, Integer>>) tuple2 -> {
                toWritePG(tuple2);
                System.out.println("粉丝从" + tuple2._1.split("@")[0] + ",直播间到" + tuple2._1.split("@")[1] + ",共" + tuple2._2 + "人");

            });
        });
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }


    public static void toWritePG(Tuple2<String, Integer> tuple2) {
        String curtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        try {
            Object[] object = new Object[5];
            object[0] = tuple2._1.split("@")[0];
            object[1] = tuple2._1.split("@")[1];
            object[2] = Integer.valueOf(tuple2._2);
            object[3] = Timestamp.valueOf(curtime);
            object[4] = Timestamp.valueOf(curtime);
            PGCopyInUtils.getinstance().PGupdate("INSERT INTO \"public\".\"liveaccesspath\"(\"liveId\", \"toLiveId\", \"userAccessCount\", \"createdAt\", \"updatedAt\") VALUES (?, ?, ?, ?, ?);", object);
        } catch (Exception e) {
            try {
                System.out.println("________getRS sql:" + e.getMessage());
                Object[] updateobject = new Object[4];
                updateobject[0] = Integer.valueOf(tuple2._2);
                updateobject[1] = Timestamp.valueOf(curtime);
                updateobject[2] = tuple2._1.split("@")[0];
                updateobject[3] = tuple2._1.split("@")[1];
                PGCopyInUtils.getinstance().PGupdate("UPDATE \"public\".\"liveaccesspath\" SET \" \"userAccessCount\" = ?, \"updatedAt\" = ? WHERE \"liveId\" = ? AND \"toLiveId\" = ?;", updateobject);
            } catch (Exception ex) {
                System.out.println("________getRS sql:" + ex.getMessage());
            }
        }
    }
}

