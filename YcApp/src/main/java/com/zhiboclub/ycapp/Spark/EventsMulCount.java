package com.zhiboclub.ycapp.Spark;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhiboclub.ycapp.Bean.EventsMesgInfo;
import com.zhiboclub.ycapp.DBopts.PGCopyInUtils;
import kafka.tools.MessageInfo;
import kafka.utils.Json;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 * 新闻网站关键指标实时统计Spark应用程序
 *
 * @author Administrator
 */
public class EventsMulCount {

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

        JavaPairDStream<String, String> accessDStream = stream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(ConsumerRecord<String, String> b) throws Exception {
                return new Tuple2<>("events", b.value());
            }
        });

        // 统计第一个指标：实时页面pv
        calculatePagePv(accessDStream);
        // 统计第二个指标：实时页面uv
//    calculatePageUv(accessDStream);
        // 统计第三个指标：实时注册用户数
//    calculateRegisterCount(lines);
        // 统计第四个指标：实时用户跳出数
//    calculateUserJumpCount(accessDStream);
        // 统计第五个指标：实时版块pv
//    calcualteSectionPv(accessDStream);

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    /**
     * 计算页面pv
     *
     * @param accessDStream
     */
    private static void calculatePagePv(JavaPairDStream<String, String> accessDStream) {
        JavaPairDStream<String, Long> pageidDStream = accessDStream.mapToPair(

                new PairFunction<Tuple2<String, String>, String, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple)
                            throws Exception {
                        EventsMesgInfo mesginfo =  JSON.parseObject(tuple._2, EventsMesgInfo.class);
                        Timestamp startTime = mesginfo.getStartTime();
                        String body = mesginfo.getBody();
                        JSONObject jb = JSON.parseObject(body);
                        String lid = mesginfo.getLiveId();
                        String uid = mesginfo.getUser().getUserId();
                        if(jb.containsKey("join")){
                            JSONObject join = JSONObject.parseObject(jb.get("join").toString());
                            String onlineCount = join.get("onlineCount").toString();
                            String pageViewCount = join.get("pageViewCount").toString();
                            String totalCount = join.get("totalCount").toString();
                            System.out.println("更新数据库"+"直播id："+lid+"；在线人数："+onlineCount+"；Pv"+pageViewCount+"；Uv："+totalCount);
                        }
                        if(jb.containsKey("txt")){
                            JSONObject txt = JSONObject.parseObject(jb.get("txt").toString());
                            String key = lid +":"+ startTime.getTime();//评论数
                            String keyuser = lid + ":"+uid+":"+startTime.getTime();//评论人数
                            System.out.println("直播id："+lid+"；评论数："+txt);
                        }
                        if(jb.containsKey("shares")){
                            System.out.println("shares");
                        }
                        if(jb.containsKey("share_goods_list")){
                            System.out.println("share_goods_list");
                        }
                        if(jb.containsKey("follow")){
                            System.out.println("follow");
                        }
                        if(jb.containsKey("trade_show")){
                            System.out.println("trade_show");
                        }
                        if(jb.containsKey("biz")){
                            System.out.println("biz");
                        }
                        else{
                            System.out.println("not watch data");
                        }

                        return new Tuple2<String, Long>(lid, 1L);
                    }

                });

        JavaPairDStream<String, Long> pagePvDStream = pageidDStream.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        pagePvDStream.print();

        // 在计算出每10秒钟的页面pv之后，其实在真实项目中，应该持久化
        // 到mysql，或redis中，对每个页面的pv进行累加
        // javaee系统，就可以从mysql或redis中，读取page pv实时变化的数据，以及曲线图
    }

    /**
     * 计算页面uv
     *
     * @param <U>
     * @param accessDStream
     */
    private static <U> void calculatePageUv(JavaPairDStream<String, String> accessDStream) {

        JavaDStream<String> pageidUseridDStream = accessDStream.map(

                new Function<Tuple2<String, String>, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public String call(Tuple2<String, String> tuple) throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");

                        Long pageid = Long.valueOf(logSplited[3]);
                        Long userid = Long.valueOf("null".equalsIgnoreCase(logSplited[2]) ? "-1" : logSplited[2]);

                        return pageid + "_" + userid;
                    }

                });

        JavaDStream<String> distinctPageidUseridDStream = pageidUseridDStream.transform(

                new Function<JavaRDD<String>, JavaRDD<String>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                        return rdd.distinct();
                    }

                });

        JavaPairDStream<Long, Long> pageidDStream = distinctPageidUseridDStream.mapToPair(

                new PairFunction<String, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Long> call(String str) throws Exception {
                        String[] splited = str.split("_");
                        Long pageid = Long.valueOf(splited[0]);
                        return new Tuple2<Long, Long>(pageid, 1L);
                    }

                });

        JavaPairDStream<Long, Long> pageUvDStream = pageidDStream.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        pageUvDStream.print();
    }

    /**
     * 计算实时注册用户数
     *
     * @param lines
     */
    private static void calculateRegisterCount(JavaPairInputDStream<String, String> lines) {
        JavaPairDStream<String, String> registerDStream = lines.filter(

                new Function<Tuple2<String, String>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");

                        String action = logSplited[5];
                        if ("register".equals(action)) {
                            return true;
                        } else {
                            return false;
                        }
                    }

                });

        JavaDStream<Long> registerCountDStream = registerDStream.count();

        registerCountDStream.print();

        // 每次统计完一个最近10秒的数据之后，不是打印出来
        // 去存储（mysql、redis、hbase），选用哪一种主要看你的公司提供的环境，以及你的看实时报表的用户以及并发数量，包括你的数据量
        // 如果是一般的展示效果，就选用mysql就可以
        // 如果是需要超高并发的展示，比如QPS 1w来看实时报表，那么建议用redis、memcached
        // 如果是数据量特别大，建议用hbase

        // 每次从存储中，查询注册数量，最近一次插入的记录，比如上一次是10秒前
        // 然后将当前记录与上一次的记录累加，然后往存储中插入一条新记录，就是最新的一条数据
        // 然后javaee系统在展示的时候，可以比如查看最近半小时内的注册用户数量变化的曲线图
        // 查看一周内，每天的注册用户数量的变化曲线图（每天就取最后一条数据，就是每天的最终数据）
    }

    /**
     * 计算用户跳出数量
     *
     * @param accessDStream
     */
    private static void calculateUserJumpCount(JavaPairDStream<String, String> accessDStream) {
        JavaPairDStream<Long, Long> useridDStream = accessDStream.mapToPair(

                new PairFunction<Tuple2<String, String>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, String> tuple)
                            throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");
                        Long userid = Long.valueOf("null".equalsIgnoreCase(logSplited[2]) ? "-1" : logSplited[2]);
                        return new Tuple2<Long, Long>(userid, 1L);
                    }

                });

        JavaPairDStream<Long, Long> useridCountDStream = useridDStream.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        JavaPairDStream<Long, Long> jumpUserDStream = useridCountDStream.filter(

                new Function<Tuple2<Long, Long>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<Long, Long> tuple) throws Exception {
                        if (tuple._2 == 1) {
                            return true;
                        } else {
                            return false;
                        }
                    }

                });

        JavaDStream<Long> jumpUserCountDStream = jumpUserDStream.count();

        jumpUserCountDStream.print();
    }

    /**
     * 版块实时pv
     *
     * @param accessDStream
     */
    private static void calcualteSectionPv(JavaPairDStream<String, String> accessDStream) {
        JavaPairDStream<String, Long> sectionDStream = accessDStream.mapToPair(

                new PairFunction<Tuple2<String, String>, String, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple)
                            throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");

                        String section = logSplited[4];

                        return new Tuple2<String, Long>(section, 1L);
                    }

                });

        JavaPairDStream<String, Long> sectionPvDStream = sectionDStream.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        sectionPvDStream.print();
    }

}