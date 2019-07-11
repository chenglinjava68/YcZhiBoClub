package com.zhiboclub.ycapp.Bean;

import java.io.Serializable;
import java.sql.Timestamp;

import lombok.Data;

@Data
public class KafkaMesgInfo implements Serializable{

	private static final long serialVersionUID = 1L;
	
	//消息的名称
	private String mesgInfoName;
	//消息所属线程
	private String mesgCurrentThread;
	//消息分区
	private Integer mesgPartition;
	//消息主题
	private String mesgTopic;
	//消息的偏移量
	private Long mesgOffect;
	//消息的时间戳
	private Timestamp timestamp;
	//消息的键
	private String mesgKey;
	//消息内容值
	private EventsMesgInfo mesgInfo;
	
}