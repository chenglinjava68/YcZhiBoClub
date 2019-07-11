package com.zhiboclub.ycapp.kafka;

import com.alibaba.fastjson.JSON;
import com.zhiboclub.ycapp.Bean.*;
import com.zhiboclub.ycapp.Bean.EventsMesgInfo;
import com.zhiboclub.ycapp.Bean.FansKafkaMesgInfo;
import com.zhiboclub.ycapp.Bean.KafkaMesgInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ConsumerRecordToBean {
	private static final String SPLITCHAR = "\u0001";

	/**
	 * event表数据规整
	 * @param b ConsumerRecord kafka记录
	 * @return 以\u0001分割的数据
	 */
	public String DataCheckAndTranslate(ConsumerRecord<String, String> b) {
		Timestamp currentTime =new Timestamp(System.currentTimeMillis());
		KafkaMesgInfo kafkaMesgInfo = new KafkaMesgInfo();
		EventsMesgInfo mesgInfo ;
		kafkaMesgInfo.setMesgCurrentThread(Thread.currentThread().toString());
		kafkaMesgInfo.setMesgInfoName("Kafka");
		kafkaMesgInfo.setMesgPartition(b.partition());
		kafkaMesgInfo.setMesgOffect(b.offset());
		kafkaMesgInfo.setMesgTopic(b.topic());
		kafkaMesgInfo.setTimestamp(new Timestamp(b.timestamp()));
		kafkaMesgInfo.setMesgKey(b.key());
		mesgInfo =JSON.parseObject(b.value(), EventsMesgInfo.class);
		kafkaMesgInfo.setMesgInfo(mesgInfo);
		mesgInfo.setUpdateTime(currentTime);
		mesgInfo.setCreateTime(kafkaMesgInfo.getTimestamp());
		String addValueToDB = UUID.randomUUID() + ":" + kafkaMesgInfo.getMesgPartition() + ":" + kafkaMesgInfo.getMesgOffect() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getLiveId() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getAnchorId() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getTopic() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getTitle() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getStartTime() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getType() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getTypeCode() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getUser().getUserId() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getUser().getUserName() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getBody() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getCreateTime() + ConsumerRecordToBean.SPLITCHAR
								+ mesgInfo.getUpdateTime();
		return addValueToDB;
	}

	/**
	 * fansinfo 和 anchorfans表的数据规整
	 * @param b ConsumerRecord kafka记录
	 * @return 以\u0001分割的数据
	 */

	public List<String> FansDataCheckAndTranslate(ConsumerRecord<String, String> b) {
		Timestamp currentTime =new Timestamp(System.currentTimeMillis());

		FansKafkaMesgInfo fansKafkaMesgInfo = JSON.parseObject(b.value(),FansKafkaMesgInfo.class);

		String addFansValueToDB = fansKafkaMesgInfo.getFansInfo().getUserId() +":"+UUID.randomUUID() + ConsumerRecordToBean.SPLITCHAR
								+ fansKafkaMesgInfo.getFansInfo().getUserId() +  ConsumerRecordToBean.SPLITCHAR
								+ fansKafkaMesgInfo.getFansInfo().getUserName() +  ConsumerRecordToBean.SPLITCHAR
								+ fansKafkaMesgInfo.getFansInfo().getUserAvatar() +  ConsumerRecordToBean.SPLITCHAR
								+ fansKafkaMesgInfo.getFansInfo().getTaoQiHi() +  ConsumerRecordToBean.SPLITCHAR
								+ fansKafkaMesgInfo.getFansInfo().getAPassUser() +  ConsumerRecordToBean.SPLITCHAR
								+ fansKafkaMesgInfo.getFansInfo().getVipUser() +  ConsumerRecordToBean.SPLITCHAR
								+ currentTime + ConsumerRecordToBean.SPLITCHAR
								+ currentTime;
		String addAnchorFansValueToDB = fansKafkaMesgInfo.getFansInfo().getUserId() +":"+ fansKafkaMesgInfo.getAnchorFans().getAnchorId()+":"+UUID.randomUUID() + ConsumerRecordToBean.SPLITCHAR
								+ fansKafkaMesgInfo.getAnchorFans().getUserId() + ConsumerRecordToBean.SPLITCHAR
								+ fansKafkaMesgInfo.getAnchorFans().getAnchorId() + ConsumerRecordToBean.SPLITCHAR
								+ fansKafkaMesgInfo.getAnchorFans().getLevel() + ConsumerRecordToBean.SPLITCHAR
								+ currentTime + ConsumerRecordToBean.SPLITCHAR
								+ currentTime;
		List<String> list = new ArrayList<>();
		list.add(addFansValueToDB);
		list.add(addAnchorFansValueToDB);
		return list;
	}

	/**
	 * 直接将kafka的数据转为对象，用于单条插入
	 * @param b
	 * @return
	 */
	public FansKafkaMesgInfo FansDataAndTranslate(ConsumerRecord<String, String> b) {
		FansKafkaMesgInfo fansKafkaMesgInfo = JSON.parseObject(b.value(),FansKafkaMesgInfo.class);

		return fansKafkaMesgInfo;
	}
}
