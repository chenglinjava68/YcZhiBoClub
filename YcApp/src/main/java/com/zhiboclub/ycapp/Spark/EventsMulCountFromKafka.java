package com.zhiboclub.ycapp.Spark;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhiboclub.ycapp.Bean.EventsMesgInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 * events关键指标实时统计Spark应用程序
 *
 * @author Administrator
 */
public class EventsMulCountFromKafka {

    public static void main(String[] args) throws Exception {
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

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        JavaDStream<String> words = stream.flatMap((FlatMapFunction<ConsumerRecord<String, String>, String>) s -> {
            List<String> list = new ArrayList<>();
            EventsMesgInfo mesginfo = JSON.parseObject(s.value(), EventsMesgInfo.class);
            Timestamp startTime = mesginfo.getStartTime();
            String body = mesginfo.getBody();
            JSONObject jb = JSON.parseObject(body);
            String lid = mesginfo.getLiveId();
            String uid = mesginfo.getUser().getUserId();
            list.add("TieFen:" + lid + ":" + uid);


            if (jb.containsKey("join")) {
                //更新直播间的pv和uv等
                JSONObject join = JSONObject.parseObject(jb.get("join").toString());
                String onlineCount = join.get("onlineCount").toString();
                String pageViewCount = join.get("pageViewCount").toString();
                String totalCount = join.get("totalCount").toString();
                list.add("pvuv:" + lid + ":@:" + onlineCount + ":" + pageViewCount + ":" + totalCount);
                System.out.println("更新数据库" + "直播id：" + lid + "；在线人数：" + onlineCount + "；Pv" + pageViewCount + "；Uv：" + totalCount);
            } else if (jb.containsKey("txt")) {
                //统计评论数和评论人数
                JSONObject txt = JSONObject.parseObject(jb.get("txt").toString());
                String key = "txtNum:" + lid + ":" + startTime.getTime();//评论数
                String keyuser = "txtPeopleNum:" + lid + ":" + uid + ":" + startTime.getTime();//评论人数
                System.out.println("直播id：" + lid + "；评论数：" + txt);
                list.add(key);
                list.add(keyuser);
            } else if (jb.containsKey("shares")) {
                System.out.println("shares");
            } else if (jb.containsKey("share_goods_list")) {
                System.out.println("share_goods_list");
            } else if (jb.containsKey("follow")) {
                System.out.println("follow");
            } else if (jb.containsKey("trade_show")) {
                System.out.println("trade_show");
            } else if (jb.containsKey("biz")) {
                System.out.println("biz");
            } else {
                System.out.println("not watch data");
            }
            return list.iterator();
        });

        // 统计第一个指标：实时页面pv，uv，online人数
        calculatePagePv(words);

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    private static void calculatePagePv(JavaDStream accessDStream) {
        JavaPairDStream<String, Integer> one = accessDStream.mapToPair((PairFunction<String, String, Integer>) t -> new Tuple2<>(t, 1));

        JavaPairDStream counts = one.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> {
            return v1 + v2;
        });
//        counts.print();

        JavaPairDStream count = counts.mapToPair(new PairFunction<Tuple2<String,Integer>,String , String>() {
            @Override
            public Tuple2 call(Tuple2<String,Integer> t) throws Exception {
                if(t._1.startsWith("pvuv")){
                    return new Tuple2(t._1.split(":@:")[0],t._1.split(":@:")[1]);
                }
                return t;
            }
        });
        JavaPairDStream countsbykey = count.reduceByKey((Function2<String, String, String>) (v1, v2) -> {
            return v2;
        });


//        方法一:VoidFunction
//        counts.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
//            @Override
//            public void call(JavaPairRDD<String,Integer> rdd) {
//                rdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                        System.out.println(stringIntegerTuple2._1);
//                    }
//                });
//            }
//        });


//        方法二:VoidFunction2
//        counts.foreachRDD(new VoidFunction2<JavaPairRDD<String,Integer>,Time>() {
//            @Override
//            public void call(JavaPairRDD<String,Integer> rdd,Time time) {
//                System.out.println(time);
//                rdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                        System.out.println(stringIntegerTuple2._1);
//                    }
//                });
//            }
//        });

//        方法三：lamba表达式
        Map<String,String> maps = new HashMap<>();
        countsbykey.foreachRDD((VoidFunction2<JavaPairRDD<String, Integer>, Time>) (rdd, time) -> {
            rdd.foreach((VoidFunction<Tuple2<String, Integer>>) tuple2 -> {
                if (tuple2._1.startsWith("pvuv")) {
//                    String[] tmp = tuple2._1.split(":@:");
//                    maps.put(tmp[0],tmp[1]);
                    System.out.println("++++++++=============");
                    System.out.println(tuple2);
                }
            });

        });

//        counts.print();

//        每60秒进行一次checkpoint，历史统计数
//        DStream<Tuple2<String, Integer>> result = one.updateStateByKey((Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (values, state) -> {
//                Integer updatedValue = 0;
//                if (state.isPresent()) {
//                    updatedValue = Integer.parseInt(state.get().toString());
//                }
//                for (Integer value : values) {
//                    updatedValue += value;
//                }
//                return Optional.of(updatedValue);
//        }).checkpoint(Durations.seconds(60));
//        result.print();


//        窗口处理5秒一次，聚合20秒内的数据
//        JavaPairDStream counts = one.reduceByKeyAndWindow((Function2<Integer, Integer, Integer>) (v1, v2) -> (v1 + v2),Durations.seconds(20), Durations.seconds(5));

//        计算当前批次的统计


//        JavaPairDStream counts = one.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
//        counts.print();
    }
}