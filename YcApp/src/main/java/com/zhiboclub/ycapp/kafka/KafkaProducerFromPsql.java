package com.zhiboclub.ycapp.kafka;

import com.alibaba.fastjson.JSON;
import com.google.gson.JsonObject;
import com.zhiboclub.ycapp.Bean.EventsMesgInfo;
import com.zhiboclub.ycapp.Bean.FansInfo;
import com.zhiboclub.ycapp.DBopts.PGCopyInUtils;
import com.zhiboclub.ycapp.Utils.ConfigurationManager;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class KafkaProducerFromPsql extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducers.class);

    private static Properties props = new Properties();

    private static String topic = "events-p3";

    private static Producer<String, String> producer = null;

    public static Producer<String, String> getInstance() {
        if (producer == null) {
            init();
            producer = new KafkaProducer<>(props);
        }
        return producer;
    }

    public static void init() {
        props.put("bootstrap.servers",
                ConfigurationManager.getInstance().GetValues("producer.bootstrap.servers", "127.0.0.1:9092"));
        props.put("acks", ConfigurationManager.getInstance().GetValues("producer.acks", "all"));
        props.put("retries", ConfigurationManager.getInstance().GetValues("producer.retries", "1"));
        props.put("batch.size", ConfigurationManager.getInstance().GetValues("producer.batch.size", "163840"));
        props.put("linger.ms", ConfigurationManager.getInstance().GetValues("producer.linger.ms", "1"));
        props.put("buffer.memory", ConfigurationManager.getInstance().GetValues("producer.buffer.memory", "33554432"));
        props.put("key.serializer", ConfigurationManager.getInstance().GetValues("producer.key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"));
        props.put("value.serializer", ConfigurationManager.getInstance().GetValues("producer.value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"));
    }

    public static void SendMessages(String key, String value) {
        ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, key, value);
        LOG.info("send message begin.");
        KafkaProducers.getInstance().send(msg, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                LOG.info("消息发送成功:key=" + key + ";value=" + value);
                try {
                    sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        KafkaProducers.getInstance().close(100, TimeUnit.MILLISECONDS);
        LOG.info("send message over.");
    }

    public static void main(String[] args) {
        LOG.info("send message begin.");
        EventsMesgInfo info = new EventsMesgInfo();
        FansInfo user = new FansInfo();
        try {
            ResultSet rs = PGCopyInUtils.getinstance().query("select * from events limit 10000");
            while (rs.next()) {
                info.setLiveId(rs.getString("liveId"));
                info.setAnchorId(rs.getString("anchorId"));
                info.setTopic(rs.getString("topic"));
                info.setTitle(rs.getString("title"));
                info.setStartTime(rs.getTimestamp("startTime"));
                info.setType(rs.getString("type"));
                user.setUserId(rs.getString("userId"));
                user.setUserName(rs.getString("userName"));
                info.setUser(user);
                info.setBody(rs.getString("body"));
                info.setCreateTime(rs.getTimestamp("createdAt"));
                info.setUpdateTime(rs.getTimestamp("updatedAt"));
                ProducerRecord<String, String> msg = new ProducerRecord<>(topic, JSON.toJSONString(info));
                KafkaProducers.getInstance().send(msg);
                System.out.println("发送消息成功：" + msg.value());
                try {
                    sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            producer.close(100, TimeUnit.MILLISECONDS);
        }
        LOG.info("send message over.");
    }

}
