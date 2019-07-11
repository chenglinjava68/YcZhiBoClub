package com.zhiboclub.ycapp.Utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeManager {

	public static String getFileTimeStr() {
		Date date = new Date(System.currentTimeMillis());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHH");
		return dateFormat.format(date);
	}

	public static void main(String[] args) {
		System.out.println(System.currentTimeMillis());
	}
}
