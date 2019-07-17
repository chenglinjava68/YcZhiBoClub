package com.zhiboclub.ycapp.Spark;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhiboclub.ycapp.Bean.EventsMesgInfo;
import com.zhiboclub.ycapp.DBopts.PGCopyInUtils;
import com.zhiboclub.ycapp.Utils.TimeManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
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
public class EventsMulCounts {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("YcKafka2SparkUserFlow").setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("/streaming_checkpoint");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "127.0.0.1:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test2");
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

            list.add("userPath:" + lid + ":" + startTime.getTime() + ":@:" + uid);
//            list.add("userPath:" + lid + ":" + startTime.getTime() + ":@:" + uid+":"+mesginfo.getCreateTime());
//            list.add("userPath:" + uid + ":@:" + lid+":"+startTime.getTime());

            if (jb.containsKey("join")) {
                //更新直播间的pv和uv等
                JSONObject join = JSONObject.parseObject(jb.get("join").toString());
                String onlineCount = join.get("onlineCount").toString();
                String pageViewCount = join.get("pageViewCount").toString();
                String totalCount = join.get("totalCount").toString();
                list.add("pvuv:" + lid +":"+startTime.getTime()+ ":@:" + onlineCount + ":" + pageViewCount + ":" + totalCount);
//                System.out.println("更新数据库" + "直播id：" + lid + "；在线人数：" + onlineCount + "；Pv" + pageViewCount + "；Uv：" + totalCount);
            } else if (jb.containsKey("txt")) {
                //统计评论数和评论人数
                JSONObject txt = JSONObject.parseObject(jb.get("txt").toString());
                String key = "txtNum:" + lid + ":" + startTime.getTime();//评论数
                String keyuser = "txtPeopleNum:" + lid + ":" + startTime.getTime() + ":@:" + uid;//评论人数
//                System.out.println("直播id：" + lid + "；评论数：" + txt);
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
//        calculatePagePv(words);

        // 统计评论数和评论人数
//        calculateTxt(words);

        //统计直播间粉丝流向
        calculateUserPath(words);

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }

    private static void calculatePagePv(JavaDStream accessDStream) {

        accessDStream = accessDStream.filter((Function<String, Boolean>) v1 -> {
            if (v1.startsWith("pvuv"))
                return true;
            return false;
        });
        //转换为(key,1),便于统计去重
        JavaPairDStream<String, Integer> one = accessDStream.mapToPair((PairFunction<String, String, Integer>) t -> new Tuple2<>(t, 1));
        //统计相同key下，value求和
        JavaPairDStream counts = one.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
        //将上一个(key,value)，转换成新的(key,value)
        JavaPairDStream count = counts.mapToPair((PairFunction<Tuple2<String, Integer>, String, String>) t -> {
            if (t._1.startsWith("pvuv")) {
                return new Tuple2(t._1.split(":@:")[0], t._1.split(":@:")[1]);
            }
            return new Tuple2<>(t._1, t._2.toString());
        });
        //相同的key，只取最新的value
        JavaPairDStream countsbykey = count.reduceByKey((Function2<String, String, String>) (v1, v2) -> v2);

        countsbykey.foreachRDD((VoidFunction2<JavaPairRDD<String, String>, Time>) (rdd, time) -> {
            rdd.foreach((VoidFunction<Tuple2<String, String>>) tuple2 -> {
                if (tuple2._1.startsWith("pvuv")) {
                    System.out.println("更新数据库为" + tuple2);
                    String curtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                    try{
                        Object[] object = new Object[7];
                        object[0] = tuple2._1.split(":")[1];
                        object[1] = Timestamp.valueOf(TimeManager.timeStampToTime(tuple2._1.split(":")[2]));
                        object[2] = Integer.parseInt(tuple2._2.split(":")[0]);
                        object[3] = Integer.parseInt(tuple2._2.split(":")[1]);
                        object[4] = Integer.parseInt(tuple2._2.split(":")[2]);
                        object[5] = Timestamp.valueOf(curtime);
                        object[6] = Timestamp.valueOf(curtime);
                        PGCopyInUtils.getinstance().PGupdate("INSERT INTO \"public\".\"eventscount\"(\"liveId\", \"startTime\", \"onlineCount\", \"pageViewCount\", \"totalCount\", \"createdAt\", \"updatedAt\") VALUES (?, ?, ?, ?, ?, ?, ?);",object);
                    }catch (Exception e){
                        System.out.println("________getRS sql:" + e.getMessage());
                        Object[] updateobject = new Object[5];
                        updateobject[0] = Integer.parseInt(tuple2._2.split(":")[0]);
                        updateobject[1] = Integer.parseInt(tuple2._2.split(":")[1]);
                        updateobject[2] = Integer.parseInt(tuple2._2.split(":")[2]);
                        updateobject[3] = Timestamp.valueOf(curtime);
                        updateobject[4] = tuple2._1.split(":")[1];
                        PGCopyInUtils.getinstance().PGupdate("UPDATE \"public\".\"eventscount\" SET \"onlineCount\" = ?, \"pageViewCount\" = ?, \"totalCount\" = ?,  \"updatedAt\" = ? WHERE \"liveId\"=?",updateobject);
                    }
                }
            });
        });
    }


    private static void calculateTxt(JavaDStream accessDStream) {
        accessDStream = accessDStream.filter((Function<String, Boolean>) v1 -> {
            if (v1.startsWith("txt"))
                return true;
            return false;
        });
        //转换为(key,1),便于统计去重
        JavaPairDStream<String, Integer> one = accessDStream.mapToPair((PairFunction<String, String, Integer>) t -> new Tuple2<>(t, 1));
        //统计相同key下，value求和
        JavaPairDStream<String, Integer> counts = one.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        JavaPairDStream<String, Integer> count = counts.mapToPair((PairFunction<Tuple2<String, Integer>, String, Integer>) t -> {

            if (t._1.startsWith("txtPeopleNum")) {
                return new Tuple2(t._1, 1);
            }
            return new Tuple2(t._1, t._2);
        });

//        JavaPairDStream<String, Integer> count2 = count.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        JavaPairDStream<String, Integer> total = count.updateStateByKey((Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (values, state) -> {
            Integer updatedValue = 0;
            if (state.isPresent()) {
                updatedValue = Integer.parseInt(state.get().toString());
            }
            for (Integer value : values) {
                updatedValue += value;
            }
            return Optional.of(updatedValue);
        });

        JavaPairDStream txtPeopleNumTotal = total.mapToPair((PairFunction<Tuple2<String, Integer>, String, Integer>) t -> {

            if (t._1.startsWith("txtPeopleNum")) {
                return new Tuple2(t._1.split(":@:")[0], 1);
            }
            return new Tuple2(t._1, t._2);
        });

        JavaPairDStream<String, Integer> countf = txtPeopleNumTotal.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        countf.foreachRDD((VoidFunction2<JavaPairRDD<String, Integer>, Time>) (rdd, time) -> {

            rdd.foreach((VoidFunction<Tuple2<String, Integer>>) tuple2 -> {
                String curtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                if (tuple2._1.startsWith("txtNum")) {
                    System.out.println("更新数据库评论数" + tuple2);
                    try{
                        Object[] object = new Object[5];
                        object[0] = tuple2._1.split(":")[1];
                        object[1] = Timestamp.valueOf(tuple2._1.split(":")[2]);
                        object[2] = tuple2._2;
                        object[3] = Timestamp.valueOf(curtime);
                        object[4] = Timestamp.valueOf(curtime);
                        PGCopyInUtils.getinstance().PGupdate("INSERT INTO \"public\".\"eventscount\"(\"liveId\", \"startTime\", \"txtNum\", \"createdAt\", \"updatedAt\") VALUES (?, ?, ?, ?, ?);",object);
                    }catch (Exception e){
                        System.out.println("________getRS sql:" + e.getMessage());
                        Object[] updateobject = new Object[3];
                        updateobject[0] = tuple2._2;
                        updateobject[1] = Timestamp.valueOf(curtime);
                        updateobject[2] = tuple2._1.split(":")[1];
                        PGCopyInUtils.getinstance().PGupdate("UPDATE \"public\".\"eventscount\" SET \"txtNum\" = ? ,\"updatedAt\" = ? WHERE \"liveId\" = ?;",updateobject);
                    }
                }
                if (tuple2._1.startsWith("txtPeopleNum")) {
                    System.out.println("更新数据库评论人数" + tuple2);
                    try{
                        Object[] object = new Object[5];
                        object[0] = tuple2._1.split(":")[1];
                        object[1] = Timestamp.valueOf(tuple2._1.split(":")[2]);
                        object[2] = tuple2._2;
                        object[3] = Timestamp.valueOf(curtime);
                        object[4] = Timestamp.valueOf(curtime);
                        PGCopyInUtils.getinstance().PGupdate("INSERT INTO \"public\".\"eventscount\"(\"liveId\", \"startTime\", \"txtPeopleNum\", \"createdAt\", \"updatedAt\") VALUES (?, ?, ?, ?, ?);",object);
                    }catch (Exception e){
                        System.out.println("________getRS sql:" + e.getMessage());
                        Object[] updateobject = new Object[3];
                        updateobject[0] = tuple2._2;
                        updateobject[1] = Timestamp.valueOf(curtime);
                        updateobject[2] = tuple2._1.split(":")[1];
                        PGCopyInUtils.getinstance().PGupdate("UPDATE \"public\".\"eventscount\" SET \"txtPeopleNum\" = ? ,\"updatedAt\" = ? WHERE \"liveId\" = ?;",updateobject);
                    }
                }



            });
        });
    }

    /**
     * 监控某些liveId的用户行为
     */
    private static void calculateUserPath(JavaDStream accessDStream) {

        List<String> monLiveId = new ArrayList<>();
        monLiveId.add("230703875007");
        monLiveId.add("230734135125");
        monLiveId.add("230577279604");

        accessDStream = accessDStream.filter((Function<String, Boolean>) v1 -> {
            if (v1.startsWith("userPath")) {
                if (monLiveId.contains(v1.split(":")[1]))
                    return true;
            }
            return false;
        });

        JavaPairDStream<String, String> asplit = accessDStream.mapToPair((PairFunction<String, String, String>) word -> {
            return new Tuple2<>(word.split(":@:")[0], word.split(":@:")[1]);
        });


        JavaPairDStream<String, Iterable<String>> total = asplit.groupByKey();

        total.foreachRDD((VoidFunction2<JavaPairRDD<String, Iterable<String>>, Time>) (rdd, time) ->
        {
            rdd.foreach((VoidFunction<Tuple2<String, Iterable<String>>>) tuple2 -> {

                HashSet<String> uIdonly = new HashSet();
                Iterator iter = tuple2._2.iterator();
                while (iter.hasNext()) {
                    String str = (String) iter.next();
                    uIdonly.add(str);
                }

                StringBuilder sb = new StringBuilder();
                sb.append("select count(\"liveId\") total ,\"liveId\" from (select DISTINCT \"userId\",\"liveId\" from events where ");
                for (String i : uIdonly) {
                   sb.append("\"userId\"="+ i + " or ");
                }
                sb.delete(sb.length()-3,sb.length()-1);
                sb.append(" and \"startTime\" < '"+TimeManager.timeStampToTime(tuple2._1.split(":")[2])+"')a GROUP BY \"liveId\" ORDER BY total desc limit 10");
                System.out.println(sb.toString());
                ResultSet rs = PGCopyInUtils.getinstance().query(sb.toString());
                Map<String,Integer> map = new HashMap<>();
                while(rs.next()){
//                    System.out.println("我的直播ID:"+tuple2._1+"直播ID:"+rs.getString("liveId")+"总计:"+rs.getString("total"));
                    map.put(rs.getString("liveId"),rs.getInt("total"));
                }
                String curtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                try{
                    Object[] object = new Object[5];
                    object[0] = tuple2._1.split(":")[1];
                    object[1] = Timestamp.valueOf(tuple2._1.split(":")[2]);
                    object[2] = JSON.toJSONString(map);
                    object[3] = Timestamp.valueOf(curtime);
                    object[4] = Timestamp.valueOf(curtime);
                    PGCopyInUtils.getinstance().PGupdate("INSERT INTO \"public\".\"eventscount\"(\"liveId\", \"startTime\", \"userLivePath\", \"createdAt\", \"updatedAt\") VALUES (?, ?, ?, ?, ?);",object);
                }catch (Exception e){
                    System.out.println("________getRS sql:" + e.getMessage());
                    Object[] updateobject = new Object[3];
                    updateobject[0] = JSON.toJSONString(map);
                    updateobject[1] = Timestamp.valueOf(curtime);
                    updateobject[2] = tuple2._1.split(":")[1];
                    PGCopyInUtils.getinstance().PGupdate("UPDATE \"public\".\"eventscount\" SET \"userLivePath\" = ? ,\"updatedAt\" = ? WHERE \"liveId\" = ?;",updateobject);
                }
            });
        });
    }
}