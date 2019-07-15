package com.zhiboclub.ycapp.Utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeManager {

	public static String getFileTimeStr() {
		Date date = new Date(System.currentTimeMillis());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHH");
		return dateFormat.format(date);
	}

	/**
	 * 时间戳转换成时间
	 * @param s 传入的时间戳
	 * @return 返回格式化时间
	 */
	public static String timeStampToTime(String s){
		if(s.length() < 13){
			System.out.println("时间长度有问题");
		}
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long lt = new Long(s);
		Date date = new Date(lt);
		String res = simpleDateFormat.format(date);
		return res;
	}

	/**
	 * 时间转换成时间戳
	 * @param s 传入的时间
	 * @return 返回时间戳
	 * @throws ParseException
	 */
	public static String timeTotimeStamp(String s) throws ParseException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = simpleDateFormat.parse(s);
		long ts = date.getTime();
		String res = String.valueOf(ts);
		return res;
	}
	public static void main(String[] args) {
		System.out.println(timeStampToTime("1562894979000"));
//		System.out.println(System.currentTimeMillis());
	}
}
