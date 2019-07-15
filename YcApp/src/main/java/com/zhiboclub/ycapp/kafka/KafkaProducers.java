package com.zhiboclub.ycapp.kafka;

import com.alibaba.fastjson.JSON;
import com.zhiboclub.ycapp.Bean.EventsMesgInfo;
import com.zhiboclub.ycapp.Utils.ConfigurationManager;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducers extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducers.class);

    private static Properties props = new Properties();

    private static String topic = ConfigurationManager.getInstance().GetValues("producer.topic", "testp3");

    private static Producer<String, String> producer = null;

    public static Producer<String, String> getInstance() {
        if (producer == null) {
            init();
            producer = new KafkaProducer<String, String>(props);
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
        List<String> l = new ArrayList();
        l.add("53224092");
        l.add("713004165");
        l.add("799282621");
        List<String> lids = new ArrayList();
        lids.add("230734135125");
        lids.add("230577279604");
        lids.add("230703875007");
        for (int i = 1; i <= 100000; i++) {
            for (String lid:lids)
            for(String li:l) {
                ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, "{\"liveId\":\""+lid+"\",\"anchorId\":\"3599178445\",\"topic\":\"e3d45e51-41dc-4f1e-af47-a22b54a3fe37\",\"title\":\"二手LV古驰香奈儿\",\"type\":\"txt\",\"typeCode\":10005,\"user\":{\"userId\":\""+li+"\",\"userName\":\"mts_hua\"},\"body\":{\"txt\":{\"totalCount\":" + i + ",\"onlineCount\":" + i + ",\"addUsers\":{\"369828135\":\"mts_hua\",\"866264412\":\"tb1833817_2012\"},\"pageViewCount\":\"" + i + "\"}},\"startTime\":1562894979000}");
                ProducerRecord<String, String> msg1 = new ProducerRecord<String, String>(topic, "{\"liveId\":\""+lid+"\",\"anchorId\":\"3599178445\",\"topic\":\"e3d45e51-41dc-4f1e-af47-a22b54a3fe37\",\"title\":\"二手LV古驰香奈儿\",\"type\":\"join\",\"typeCode\":10005,\"user\":{\"userId\":\""+li+"\",\"userName\":\"mts_hua\"},\"body\":{\"join\":{\"totalCount\":" + i + ",\"onlineCount\":" + i + ",\"addUsers\":{\"369828135\":\"mts_hua\",\"866264412\":\"tb1833817_2012\"},\"pageViewCount\":\"" + i + "\"}},\"startTime\":1562894979000}");

                KafkaProducers.getInstance().send(msg);
                KafkaProducers.getInstance().send(msg1);
                System.out.println("发送消息成功：" + msg.value());
                System.out.println("发送消息成功：" + msg1.value());
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close(100, TimeUnit.MILLISECONDS);
        LOG.info("send message over.");
    }

}
