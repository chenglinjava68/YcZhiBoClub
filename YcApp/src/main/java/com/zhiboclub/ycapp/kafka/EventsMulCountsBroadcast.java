package com.zhiboclub.ycapp.kafka;

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
import org.apache.spark.broadcast.Broadcast;
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
public class EventsMulCountsBroadcast {

    //监控的主播ID
    private static HashSet<String> MonLiveId = new HashSet<>();

    //正在直播的观看用户ID
    private static HashSet<String> AccessingUser = new HashSet<>();

    //已经访问过正在直播的观看用户ID,还未离开直播间
    private static HashSet<String> AccessedUser = new HashSet<>();

    //已经访问过正在直播的观看用户ID，已离开直播间
    private static HashSet<String> DepartureUser = new HashSet<>();

    //历史访问直播间ID
    private static List<String> HistoryAccess = new ArrayList<>();

    //粉丝离开去的直播间ID
    private static List<String> DepartureAccess = new ArrayList<>();

    static JavaStreamingContext jssc;
    static {
        MonLiveId.add("230703875007");
        MonLiveId.add("230734135125");
        MonLiveId.add("230577279604");
    }
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("YcKafka2SparkUserFlow").setMaster("local[4]");

        jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("/streaming_checkpoint");
        jssc.sparkContext().broadcast(MonLiveId);
        jssc.sparkContext().broadcast(AccessingUser);
        jssc.sparkContext().broadcast(AccessedUser);
        jssc.sparkContext().broadcast(DepartureUser);
        jssc.sparkContext().broadcast(HistoryAccess);
        jssc.sparkContext().broadcast(DepartureAccess);



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
        JavaDStream<String> words = stream.flatMap((FlatMapFunction<ConsumerRecord<String, String>, String>) s -> {
            List<String> list = new ArrayList<>();
            EventsMesgInfo mesginfo = JSON.parseObject(s.value(), EventsMesgInfo.class);

            Timestamp startTime = mesginfo.getStartTime();
            String body = mesginfo.getBody();
            JSONObject jb = JSON.parseObject(body);
            String lid = mesginfo.getLiveId();
            String uid = mesginfo.getUser().getUserId();

            list.add("userPath:@:" + lid + "@" + uid);

            if (jb.containsKey("join")) {
                //更新直播间的pv和uv等
                JSONObject join = JSONObject.parseObject(jb.get("join").toString());
                String onlineCount = join.get("onlineCount").toString();
                String pageViewCount = join.get("pageViewCount").toString();
                String totalCount = join.get("totalCount").toString();
                list.add("pvuv:" + lid + ":" + startTime.getTime() + ":@:" + onlineCount + ":" + pageViewCount + ":" + totalCount);
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
                    try {
                        Object[] object = new Object[7];
                        object[0] = tuple2._1.split(":")[1];
                        object[1] = Timestamp.valueOf(TimeManager.timeStampToTime(tuple2._1.split(":")[2]));
                        object[2] = Integer.parseInt(tuple2._2.split(":")[0]);
                        object[3] = Integer.parseInt(tuple2._2.split(":")[1]);
                        object[4] = Integer.parseInt(tuple2._2.split(":")[2]);
                        object[5] = Timestamp.valueOf(curtime);
                        object[6] = Timestamp.valueOf(curtime);
                        PGCopyInUtils.getinstance().PGupdate("INSERT INTO \"public\".\"eventscount\"(\"liveId\", \"startTime\", \"onlineCount\", \"pageViewCount\", \"totalCount\", \"createdAt\", \"updatedAt\") VALUES (?, ?, ?, ?, ?, ?, ?);", object);
                    } catch (Exception e) {
                        System.out.println("________getRS sql:" + e.getMessage());
                        Object[] updateobject = new Object[5];
                        updateobject[0] = Integer.parseInt(tuple2._2.split(":")[0]);
                        updateobject[1] = Integer.parseInt(tuple2._2.split(":")[1]);
                        updateobject[2] = Integer.parseInt(tuple2._2.split(":")[2]);
                        updateobject[3] = Timestamp.valueOf(curtime);
                        updateobject[4] = tuple2._1.split(":")[1];
                        PGCopyInUtils.getinstance().PGupdate("UPDATE \"public\".\"eventscount\" SET \"onlineCount\" = ?, \"pageViewCount\" = ?, \"totalCount\" = ?,  \"updatedAt\" = ? WHERE \"liveId\"=?", updateobject);
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
                    try {
                        Object[] object = new Object[5];
                        object[0] = tuple2._1.split(":")[1];
                        object[1] = Timestamp.valueOf(tuple2._1.split(":")[2]);
                        object[2] = tuple2._2;
                        object[3] = Timestamp.valueOf(curtime);
                        object[4] = Timestamp.valueOf(curtime);
                        PGCopyInUtils.getinstance().PGupdate("INSERT INTO \"public\".\"eventscount\"(\"liveId\", \"startTime\", \"txtNum\", \"createdAt\", \"updatedAt\") VALUES (?, ?, ?, ?, ?);", object);
                    } catch (Exception e) {
                        System.out.println("________getRS sql:" + e.getMessage());
                        Object[] updateobject = new Object[3];
                        updateobject[0] = tuple2._2;
                        updateobject[1] = Timestamp.valueOf(curtime);
                        updateobject[2] = tuple2._1.split(":")[1];
                        PGCopyInUtils.getinstance().PGupdate("UPDATE \"public\".\"eventscount\" SET \"txtNum\" = ? ,\"updatedAt\" = ? WHERE \"liveId\" = ?;", updateobject);
                    }
                }
                if (tuple2._1.startsWith("txtPeopleNum")) {
                    System.out.println("更新数据库评论人数" + tuple2);
                    try {
                        Object[] object = new Object[5];
                        object[0] = tuple2._1.split(":")[1];
                        object[1] = Timestamp.valueOf(tuple2._1.split(":")[2]);
                        object[2] = tuple2._2;
                        object[3] = Timestamp.valueOf(curtime);
                        object[4] = Timestamp.valueOf(curtime);
                        PGCopyInUtils.getinstance().PGupdate("INSERT INTO \"public\".\"eventscount\"(\"liveId\", \"startTime\", \"txtPeopleNum\", \"createdAt\", \"updatedAt\") VALUES (?, ?, ?, ?, ?);", object);
                    } catch (Exception e) {
                        System.out.println("________getRS sql:" + e.getMessage());
                        Object[] updateobject = new Object[3];
                        updateobject[0] = tuple2._2;
                        updateobject[1] = Timestamp.valueOf(curtime);
                        updateobject[2] = tuple2._1.split(":")[1];
                        PGCopyInUtils.getinstance().PGupdate("UPDATE \"public\".\"eventscount\" SET \"txtPeopleNum\" = ? ,\"updatedAt\" = ? WHERE \"liveId\" = ?;", updateobject);
                    }
                }


            });
        });
    }

    /**
     * 监控某些liveId的用户行为
     */
    private static void calculateUserPath(JavaDStream accessDStream) {

        //消息结构："userPath:@:lid@uid"

        //过滤符合条件的事件
        accessDStream = accessDStream.filter((Function<String, Boolean>) v1 -> {
            if (v1.startsWith("userPath")) {
                //[ liveid1@userId@liveid2 liveid1@userId]
                if (DepartureAccess.contains(v1.split(":@:")[1])) {
                    System.out.println("丢弃1：该粉丝已经离开过房间");
                    return false;
                } else {
                    if (AccessedUser.contains(v1.split(":@:")[1])) {
                        System.out.println("丢弃2：访问列表里面有相同的liveid@userid");
                        return false;
                    } else {
                        if (AccessedUser.contains(v1.split(":@:")[1].split("@")[1])) {
                            System.out.println("进入1：该粉丝访问过其他的直播间");
                            return true;
                        } else {
                            if (MonLiveId.contains(v1.split(":@:")[1].split("@")[0])) {
                                System.out.println("进入2：该liveId存在监控");
                                return true;
                            } else {
                                System.out.println("丢弃3：不再监控范围内，也没有该监控的liveId上有这个粉丝");
                                return false;
                            }
                        }

                    }
                }
            } else {
                return false;
            }
        });

//        accessDStream.print(100);

        JavaPairDStream total = accessDStream.mapToPair((PairFunction<String, String, String>) word ->
                new Tuple2<>(word.split(":@:")[1].split("@")[0], word.split(":@:")[1].split("@")[1])
        ).groupByKey();

        total.print(100);

        total.foreachRDD((VoidFunction2<JavaPairRDD<String, Iterable<String>>, Time>) (rdd, time) ->
        {
            rdd.foreach((VoidFunction<Tuple2<String, Iterable<String>>>) tuple2 -> {
                HashSet<String> uIdonly = new HashSet();
                Iterator iter = tuple2._2.iterator();
                while (iter.hasNext()) {
                    String str = (String) iter.next();
                    uIdonly.add(str);
                }
                for(String i:uIdonly)
                if (AccessedUser.contains(i)) {
                    if (MonLiveId.contains(tuple2._1)) {
                        System.out.println("即在监控中，也在访问中");
                        AccessingUser.add(tuple2._1 + "@" + i);
                        AccessingUser.add(i);
                        System.out.println("查询数据库用户ID为" + i + "；并将查询的结果放到HistoryAccess中去");
                        ResultSet rs = PGCopyInUtils.getinstance().query("select distinct \"liveId\" from events where \"userId\" = "+i+" ORDER BY \"startTime\" desc limit 10;");
                        if (rs != null){
                            while (rs.next()) {
                                System.out.println("我的直播ID:" + tuple2._1 + "之前去过直播ID:" + rs.getString("liveId"));
                                HistoryAccess.add(tuple2._1+"@"+i+"@"+rs.getString("liveId"));
                            }
                            AccessedUser.addAll(AccessingUser);
                            for (String id : AccessedUser) {
                                if(id.split("@").length == 1){
                                    continue;
                                }
                                if (id.split("@")[1].equals(i) && !id.split("@")[0].equals(tuple2._1)) {
                                    AccessedUser.remove(id);
                                    AccessingUser.remove(id);
                                    DepartureAccess.add(id + "@" + tuple2._1);
                                    DepartureAccess.add(id);
                                    System.out.println("这个粉丝访问该直播间已经离开去了其他监控的直播间" + id + "@" + tuple2._1);
                                    break;
                                }
                            }
                        }
                    } else {
                        //在访问列表c中，不在监控a中，说明这是一个离开的event
                        System.out.println("不在监控中，在访问中");
                        for (String id : AccessedUser) {
                            if (id.split("@")[1].equals(i)) {
                                AccessedUser.remove(id);
                                AccessedUser.remove(i);
                                AccessingUser.remove(id);
                                AccessingUser.remove(i);
                                DepartureAccess.add(id + "@" + tuple2._1);
                                DepartureAccess.add(id);
                                System.out.println("这个粉丝访问该直播间已经离开去了" + id + "@" + tuple2._1);
                                break;
                            }
                        }
                    }
                } else {
                    if (MonLiveId.contains(tuple2._1)) {
                        System.out.println("处于监控的LiveID，属于第一次访问这场直播");
                        AccessingUser.add(tuple2._1 + "@" + i);
                        AccessingUser.add(i);
                        System.out.println("查询数据库用户ID为" + i+ "；并将查询的结果放到HistoryAccess中去");
                        ResultSet rs = PGCopyInUtils.getinstance().query("select distinct \"liveId\" from events where \"userId\" = "+i+" ORDER BY \"startTime\" desc limit 10;");
                        if(rs != null){
                            while (rs.next()) {
                                System.out.println("我的直播ID:" + tuple2._1 + "之前去过直播ID:" + rs.getString("liveId"));
                                HistoryAccess.add(tuple2._1+"@"+i+"@"+rs.getString("liveId"));
                            }
                            AccessedUser.addAll(AccessingUser);
                        }
                    }
                }
            });

            jssc.sparkContext().broadcast(AccessingUser);
            jssc.sparkContext().broadcast(AccessedUser);
            jssc.sparkContext().broadcast(HistoryAccess);
            jssc.sparkContext().broadcast(DepartureAccess);
            for(String i:AccessingUser){
                System.out.println("正在访问+++++++"+i);
            }
            for(String i:AccessedUser){
                System.out.println("访问过了+++++++"+i);
            }
            for(String i:HistoryAccess){
                System.out.println("历史访问+++++++"+i);
            }
            for(String i:DepartureAccess){
                System.out.println("离开到哪+++++++"+i);
            }





        });

    }
}