package com.zhiboclub.ycapp.Bean;

import java.io.Serializable;
import java.sql.Timestamp;

import lombok.Data;

@Data
public class EventsMesgInfo implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String liveId;
	private String anchorId;
	private String topic;
	private String title;
	private Timestamp startTime;
	private String type;
	private Integer typeCode;
	private FansInfo user;
	private String body;
	private Timestamp createTime;
	private Timestamp updateTime;
}
