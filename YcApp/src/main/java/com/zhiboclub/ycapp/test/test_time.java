package com.zhiboclub.ycapp.test;

import com.alibaba.fastjson.JSON;
import com.zhiboclub.ycapp.Bean.EventsMesgInfo;

import java.text.SimpleDateFormat;
import java.util.Date;

public class test_time {
	
	public static String getFileTimeStr() {
		Date date = new Date(System.currentTimeMillis());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHH");
		return dateFormat.format(date);
	}
	public static void main(String[] args) {
//		System.out.println(test_time.getFileTimeStr());

		String json = "{\"liveId\":\"230400232970\",\"anchorId\":\"3599178445\",\"topic\":\"e3d45e51-41dc-4f1e-af47-a22b54a3fe37\",\"title\":\"二手LV古驰香奈儿\",\"type\":\"join\",\"typeCode\":10005,\"user\":{\"userId\":\"134902307\",\"userName\":\"mts_hua\"},\"body\":{\"join\":{\"totalCount\":1178,\"onlineCount\":47,\"addUsers\":{\"369828135\":\"mts_hua\",\"866264412\":\"tb1833817_2012\"},\"pageViewCount\":\"12026\"}},\"startTime\":1562894979000}";

		EventsMesgInfo info = JSON.parseObject(json, EventsMesgInfo.class);
		System.out.println(json);
		System.out.println(info);
		System.out.println(info.getStartTime());
	}
}
