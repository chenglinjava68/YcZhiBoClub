package com.zhiboclub.ycapp.test;



import com.alibaba.fastjson.JSON;
import com.zhiboclub.ycapp.Bean.KafkaMesgInfo;
import com.zhiboclub.ycapp.Bean.EventsMesgInfo;
import com.zhiboclub.ycapp.Bean.FansInfo;

public class test_serializable {
	public static void main(String[] args) {
		KafkaMesgInfo msg = new KafkaMesgInfo();
		msg.setMesgTopic("test");
		EventsMesgInfo mesginfo = new EventsMesgInfo();
		FansInfo fansInfo = new FansInfo();
//		mesginfo.setId("1");
//		mesginfo.setCreatid("3");
//		msg.setMesgInfo(mesginfo);
//		System.out.println(mesginfo.getJsonStr());
		System.out.println(JSON.toJSONString(msg));
		String js = "{\"liveId\":\"230166472083\",\"anchorId\":\"2412628845\",\"topic\":\"cea210e1-0177-429d-a62c-4f50dac3709c\",\"title\":\"银来银往阅尽潮流银饰\",\"startTime\":1562297406254,\"type\":\"join\",\"typeCode\":10005,\"user\":{\"userId\":\"2808070462\",\"userName\":\"李雅921018\"},\"body\":{\"join\":{\"totalCount\":450,\"onlineCount\":4,\"addUsers\":{\"2808070462\":\"李雅921018\"},\"pageViewCount\":\"572\"}}}";
		System.out.println(JSON.parseObject(js, EventsMesgInfo.class).getUser());
//		System.out.println(JsonStrManager.getJsonStr(JsonStrManager.getJsonStr(msg)).getString("mesgInfo"));
//		KafkaMesgInfo l = JSON.parseObject(js,KafkaMesgInfo.class);
//		System.out.println(l.getMesgInfo().getId());
//		System.out.println(msg.getJsonObj());
	}
	
}
