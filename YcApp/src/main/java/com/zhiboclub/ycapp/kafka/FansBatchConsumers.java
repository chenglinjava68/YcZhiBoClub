package com.zhiboclub.ycapp.kafka;

import com.zhiboclub.ycapp.DBopts.PGCopyInUtils;
import com.zhiboclub.ycapp.ErrorUtils.CopyInErrorToFile;
import com.zhiboclub.ycapp.Utils.ConfigurationManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FansBatchConsumers {

    private static final Logger LOG = LoggerFactory.getLogger(FansBatchConsumers.class);


    private static final String fansPropertiesFile = "YcApp/conf/fans_consumer.properties";
    private static final int RETRYNUM = Integer.valueOf(ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "app.retrynum", "3"));
    private static final int GLOBAL_RETRY = Integer.valueOf(ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "app.global.retry", "100"));
    private static final int ISNEXTRETRY = Integer.valueOf(ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "app.isnextretry", "10"));
    private static final String SPLITCHAR = "\u0001,\u0001";

    private static final String TABLENAME1 = ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "gsql.tablename1", "test");
    private static final String TABLENAME2 = ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "gsql.tablename2", "test");

    private static String topic = ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "topic", "testp3");

    private static final int minBatchSize = Integer.valueOf(ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "minBatchSize", "1"));

    private static Properties props = null;

    public static Properties init() {
        if (props == null) {
            props = new Properties();
            props.put("bootstrap.servers",
                    ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "bootstrap.servers", "localhost:9092"));
            props.put("group.id", ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "group.id", "test"));
            props.put("enable.auto.commit",
                    ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "enable.auto.commit", "false"));
            props.put("auto.commit.interval.ms",
                    ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "auto.commit.interval.ms", "1000"));
            props.put("session.timeout.ms",
                    ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "session.timeout.ms", "30000"));
            props.put("key.deserializer", ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "key.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer"));
            props.put("value.deserializer", ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "value.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer"));
            props.put("max.poll.records", ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "max.poll.records", "1"));
            props.put("auto.offset.reset",
                    ConfigurationManager.getInstance().GetValues(fansPropertiesFile, "auto.offset.reset", "earliest"));
        }
        return props;
    }


    public static void main(String[] args) {

        init();
        LOG.info("Properties 配置文件初始化成功！");
        LOG.info("批量消费入库的大小：" + minBatchSize);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        LOG.info("订阅Topic成功:" + topic);
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

        int retrynum = 0;
        int global_retry = 0;
        // 设置是否未满最小批次数据量进行入口
        Boolean isnext = false;
        int isNextRetry = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.parse("PT1S"));
                StringBuilder sbfans = new StringBuilder();
                StringBuilder sbanchor = new StringBuilder();

                // 在生产较为缓慢的时候，尽量保证入库的时间不会因为未达到批处理条数而等待入库满足后入库
                if (records.count() == 0) {
                    LOG.info("正在拉取数据，请稍等...");
                    if (buffer.size() == 0) {
                        continue;
                    } else {
                        if (isNextRetry > FansBatchConsumers.ISNEXTRETRY) {
                            isnext = true;
                            isNextRetry = 0;
                        } else {
                            isNextRetry += 1;
                        }
                    }
                } else {
                    isNextRetry = 0;
                }

                // 将从kafka拉取的数据加入到List中
                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);
                }
                /* 第一步：数据达到批量要求，就写入DB，同步确认offset */
                if (buffer.size() >= minBatchSize || isnext) {
                    isnext = false;
                    for (ConsumerRecord<String, String> b : buffer) {
                        try {
                            List tmp = new ConsumerRecordToBean().FansDataCheckAndTranslate(b);
                            sbanchor.append(tmp.get(1) + "\n");
                            sbfans.append(tmp.get(0) + "\n");
                        } catch (Exception e) {
                            e.printStackTrace();
                            LOG.error("Json数据转换异常，请检查数据\n错误信息:" + e.getMessage() + "\n错误原因:" + e.getCause() + "\n异常类:"
                                    + e.getClass());
                            StringBuilder sberror = new StringBuilder();
                            sberror.append(
                                    "当前线程:" + Thread.currentThread().toString() + FansBatchConsumers.SPLITCHAR
                                            + "分区:" + b.partition() + FansBatchConsumers.SPLITCHAR + "偏移量:"
                                            + b.offset() + FansBatchConsumers.SPLITCHAR + "时间戳:" + b.timestamp()
                                            + FansBatchConsumers.SPLITCHAR + "主题:" + b.topic()
                                            + FansBatchConsumers.SPLITCHAR + "消息键:" + b.key()
                                            + FansBatchConsumers.SPLITCHAR + "消息值:" + b.value() + "\n");
                            new CopyInErrorToFile().AbortDataToFile(sberror.toString());
                            continue;
                        }

                    }
                    /* 第二步：是否已经是第n批数据出现入库异常 */
                    if (global_retry < FansBatchConsumers.GLOBAL_RETRY) {
                        /* 第三步：当前批次数据入库是否已经重试了n次 */
                        while (retrynum < FansBatchConsumers.RETRYNUM) {
                            try {
                                LOG.info("正在写入数据库，当前数据批次总数为：" + buffer.size());
                                LOG.info("当前批次写入完成共：" + PGCopyInUtils.getinstance().copyFromStream(sbanchor.toString(), TABLENAME2) + "条");
                                PGCopyInUtils.getinstance().PGupdate("DELETE from anchorfans where id in (select id FROM (select *,row_number() over (partition by \"userId\", \"anchorId\" order by \"createdAt\" desc) AS ROW_NO FROM anchorfans)b WHERE b.ROW_NO>1)", null);
                                LOG.info("当前批次写入完成共：" + PGCopyInUtils.getinstance().copyFromStream(sbfans.toString(), TABLENAME1) + "条");
                                PGCopyInUtils.getinstance().PGupdate("DELETE from fansinfo where id in (select id FROM (select *,row_number() over (partition by \"userId\" order by \"createdAt\" desc) AS ROW_NO FROM fansinfo)b WHERE b.ROW_NO>1)", null);
                                LOG.info("+++++记录" + topic + "的offset值+++++");
                                for (int i = 0; i < consumer.partitionsFor(topic).size(); i++) {
                                    LOG.info(consumer.endOffsets(Arrays.asList(new TopicPartition(topic, i))) + "");
                                }
                                LOG.info("+++++++++END++++++++");
                                consumer.commitSync();
                                buffer.clear();
                                break;
                            } catch (Exception e) {
                                e.printStackTrace();
                                retrynum += 1;
                                LOG.error("写入数据库失败，正在重试第" + retrynum + "次，请稍后！");
                                if (retrynum == 3) {
                                    global_retry += 1;
                                    retrynum = 0;
                                    StringBuilder sberror = new StringBuilder();
                                    for (ConsumerRecord<String, String> b : buffer) {
                                        sberror.append(
                                                "当前线程:" + Thread.currentThread().toString() + FansBatchConsumers.SPLITCHAR
                                                        + "分区:" + b.partition() + FansBatchConsumers.SPLITCHAR + "偏移量:"
                                                        + b.offset() + FansBatchConsumers.SPLITCHAR + "时间戳:" + b.timestamp()
                                                        + FansBatchConsumers.SPLITCHAR + "主题:" + b.topic()
                                                        + FansBatchConsumers.SPLITCHAR + "消息键:" + b.key()
                                                        + FansBatchConsumers.SPLITCHAR + "消息值:" + b.value() + "\n");
                                    }
                                    new CopyInErrorToFile().AbortDataToFile(sberror.toString());
                                    LOG.error("错误批次写入文件记录");
                                    LOG.error("已经连续入库第" + global_retry + "次错误，请检查数据库连接是否正常或者字段格式是否匹配！！！");

                                    buffer.clear();
                                    break;
                                }
                            }

                        }
                    } else {
                        LOG.error("系统出错，程序退出exit(1)");
                        System.exit(1);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

}
