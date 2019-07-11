package com.zhiboclub.ycapp.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.zhiboclub.ycapp.Bean.FansKafkaMesgInfo;
import com.zhiboclub.ycapp.DBopts.PGCopyInUtils;
import com.zhiboclub.ycapp.ErrorUtils.CopyInErrorToFile;
import com.zhiboclub.ycapp.Utils.ConfigurationManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FansConsumers {

    private static final Logger LOG = LoggerFactory.getLogger(FansConsumers.class);

    // 设置数据库入库重试次数
    private static final int RETRYNUM = 3;
    // 设置批次失败重试次数
    private static final int GLOBAL_RETRY = 115;
    // 如果在最小时间内拉取的数量仍然小于最小批处理的大小，强制写入库
    private static final int ISNEXTRETRY = 10;
    //写入文件的分隔符
    private static final String SPLITCHAR = "\u0001,\u0001";

    private static final String TABLENAME = ConfigurationManager.getInstance().GetValues("postgres.tablename","t");


    public static void main(String[] args) {
        Properties props = new Properties();
        String topic = "fans-info";
        props.put("bootstrap.servers",
                ConfigurationManager.getInstance().GetValues("consumer.bootstrap.servers", "localhost:9092"));
        props.put("group.id", ConfigurationManager.getInstance().GetValues("consumer.group.id", "test"));
        props.put("enable.auto.commit",
                ConfigurationManager.getInstance().GetValues("consumer.enable.auto.commit", "false"));
        props.put("auto.commit.interval.ms",
                ConfigurationManager.getInstance().GetValues("consumer.auto.commit.interval.ms", "1000"));
        props.put("session.timeout.ms",
                ConfigurationManager.getInstance().GetValues("consumer.session.timeout.ms", "30000"));
        props.put("key.deserializer", ConfigurationManager.getInstance().GetValues("consumer.key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"));
        props.put("value.deserializer", ConfigurationManager.getInstance().GetValues("consumer.value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"));
        props.put("max.poll.records", ConfigurationManager.getInstance().GetValues("consumer.max.poll.records", "1"));
        props.put("auto.offset.reset",
                ConfigurationManager.getInstance().GetValues("consumer.auto.offset.reset", "earliest"));

        final int minBatchSize = Integer
                .parseInt(ConfigurationManager.getInstance().GetValues("consumer.minBatchSize", "1"));
        LOG.info("批量消费入库的大小：" + minBatchSize);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

        int retrynum = 0;
        int global_retry = 0;
        // 设置是否未满最小批次数据量进行入口
        Boolean isnext = false;
        int isNextRetry = 0;
        FansKafkaMesgInfo fansKafkaMesgInfo = null;

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.parse("PT1S"));
                StringBuilder sbanchor = new StringBuilder();
                StringBuilder sbfans = new StringBuilder();

                // 在生产较为缓慢的时候，尽量保证入库的时间不会因为未达到批处理条数而等待入库满足后入库
                if (records.count() == 0) {
                    LOG.info("正在拉取数据，请稍等...");
                    if (buffer.size() == 0) {
                        continue;
                    } else {
                        if (isNextRetry > ISNEXTRETRY) {
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
//                            List tmp = new ConsumerRecordToBean().FansDataCheckAndTranslate(b);
//                            sbanchor.append(tmp.get(1) + "\n");
//                            sbfans.append(tmp.get(0) + "\n");
                            fansKafkaMesgInfo = new ConsumerRecordToBean().FansDataAndTranslate(b);

                        } catch (Exception e) {
                            e.printStackTrace();
                            LOG.error("Json数据转换异常，请检查数据\n错误信息:" + e.getMessage() + "\n错误原因:" + e.getCause() + "\n异常类:"
                                    + e.getClass());
                            StringBuilder sberror = new StringBuilder();
                            sberror.append(
                                    "当前线程:" + Thread.currentThread().toString() + SPLITCHAR
                                            + "分区:" + b.partition() + SPLITCHAR + "偏移量:"
                                            + b.offset() + SPLITCHAR + "时间戳:" + b.timestamp()
                                            + SPLITCHAR + "主题:" + b.topic()
                                            + SPLITCHAR + "消息键:" + b.key()
                                            + SPLITCHAR + "消息值:" + b.value() + "\n");
                            new CopyInErrorToFile().AbortDataToFile(sberror.toString());
                            continue;
                        }

                    }
                    /* 第二步：是否已经是第n批数据出现入库异常 */
                    if (global_retry < GLOBAL_RETRY) {
                        /* 第三步：当前批次数据入库是否已经重试了n次 */
                        while (retrynum < RETRYNUM) {
                            try {
                                if (fansKafkaMesgInfo != null){

                                    try{
                                        Object[] object = new Object[3];
                                        object[0] = fansKafkaMesgInfo.getAnchorFans().getUserId();
                                        object[1] = fansKafkaMesgInfo.getAnchorFans().getAnchorId();
                                        object[2] = fansKafkaMesgInfo.getAnchorFans().getLevel();
                                        PGCopyInUtils.getinstance().PGupdate("INSERT INTO anchorfans (\"userid\", \"anchorid\", \"level\") VALUES (?, ?, ?);",object);
                                        LOG.info("新增一条AnchorFans数据成功");
                                    }catch (Exception e){
                                        Object[] object = new Object[3];
                                        object[1] = fansKafkaMesgInfo.getAnchorFans().getUserId();
                                        object[2] = fansKafkaMesgInfo.getAnchorFans().getAnchorId();
                                        object[0] = fansKafkaMesgInfo.getAnchorFans().getLevel();
                                        PGCopyInUtils.getinstance().PGupdate("UPDATE \"anchorfans\" SET \"level\" = ? WHERE \"userid\" = ? AND \"anchorid\" = ?;",object);
                                        LOG.info("更新一条AnchorFans数据成功");
                                    }

                                    try{
                                        Object[] object = new Object[6];
                                        object[0] = fansKafkaMesgInfo.getFansInfo().getUserId();
                                        object[1] = fansKafkaMesgInfo.getFansInfo().getUserName();
                                        object[2] = fansKafkaMesgInfo.getFansInfo().getUserAvatar();
                                        object[3] = fansKafkaMesgInfo.getFansInfo().getTaoQiHi();
                                        object[4] = fansKafkaMesgInfo.getFansInfo().getAPassUser();
                                        object[5] = fansKafkaMesgInfo.getFansInfo().getVipUser();
                                        PGCopyInUtils.getinstance().PGupdate("INSERT INTO \"fansInfo\"(\"userid\", \"username\", \"useravatar\", \"taoQiHi\", \"apassuser\", \"vipuser\") VALUES (?, ?, ?, ?, ?, ?);",object);
                                        LOG.info("新增一条Fans数据成功");
                                    }catch (Exception e){
                                        Object[] object = new Object[6];
                                        object[5] = fansKafkaMesgInfo.getFansInfo().getUserId();
                                        object[0] = fansKafkaMesgInfo.getFansInfo().getUserName();
                                        object[1] = fansKafkaMesgInfo.getFansInfo().getUserAvatar();
                                        object[2] = fansKafkaMesgInfo.getFansInfo().getTaoQiHi();
                                        object[3] = fansKafkaMesgInfo.getFansInfo().getAPassUser();
                                        object[4] = fansKafkaMesgInfo.getFansInfo().getVipUser();
                                        PGCopyInUtils.getinstance().PGupdate("UPDATE \"public\".\"fansInfo\" SET \"username\" = ?, \"useravatar\" = ?, \"taoQiHi\" = ?, \"apassuser\" = ?, \"vipuser\" = ? WHERE \"userid\" = ?;",object);
                                        LOG.info("更新一条Fans数据成功");
                                    }

                                    consumer.commitSync();
                                    fansKafkaMesgInfo = null;
                                    buffer.clear();
                                    break;
                                }else{
                                    break;
                                }
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
                                                "当前线程:" + Thread.currentThread().toString() + SPLITCHAR
                                                        + "分区:" + b.partition() + SPLITCHAR + "偏移量:"
                                                        + b.offset() + SPLITCHAR + "时间戳:" + b.timestamp()
                                                        + SPLITCHAR + "主题:" + b.topic()
                                                        + SPLITCHAR + "消息键:" + b.key()
                                                        + SPLITCHAR + "消息值:" + b.value() + "\n");
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
