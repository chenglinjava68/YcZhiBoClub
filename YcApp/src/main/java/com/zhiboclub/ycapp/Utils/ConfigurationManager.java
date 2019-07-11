package com.zhiboclub.ycapp.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationManager {

	private static final Logger LOG = LoggerFactory.getLogger(ConfigurationManager.class);

	private static ConfigurationManager gpc = null;

//	另一种方法：通过静态方法块加载
//	static {
//        try {
//            //获取配置文件输入流
//            InputStream in = ConfigurationManager.class
//                    .getClassLoader().getResourceAsStream("blaze.properties");
//
//            //加载配置对象
//            prop.load(in);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

	public static ConfigurationManager getInstance() {
		if (gpc == null) {
			gpc = new ConfigurationManager();
		}
		return gpc;
	}

	/**
	 * 初始化加载配置文件 默认配置文件为conf目录下的server.properties文件
	 */
//	public void init() {
//		props = new Properties();
//		String propPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
//		File serverfile = new File(propPath + "server.properties");
//		try {
//			if (serverfile.exists()) {
//				props.load(new FileInputStream(new File(propPath)));
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}

	/**
	 * 读取默认配置文件server.properties中的值，如果没有，则返回默认值
	 * 
	 * @param key          键
	 * @param defaultValue 默认值
	 * @return 值
	 */
	public String GetValues(String key, String defaultValue) {
		String rtValue = "";
		Properties fileprops = new Properties();
		String propPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
		File serverfile = new File(propPath + "server.properties");
		try {
			if (serverfile.exists()) {
				fileprops.load(new FileInputStream(serverfile));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (null == key) {
			LOG.error("key is null");
		} else {
			rtValue = fileprops.getProperty(key);
		}
		if (null == rtValue) {
			LOG.warn("Properties getValues return null, key is " + key);
			rtValue = defaultValue;
		}
		LOG.info("Current Properties getValues: key is " + key + "; Value is " + rtValue);
		return rtValue;
	}

	/**
	 * 给定相关的配置文件路径和key，返回响应的值
	 * 
	 * @param filepathname 配置文件路径名
	 * @param key          键
	 * @param defaultValue 默认值
	 * @return 值
	 */
	public String GetValues(String filepathname, String key, String defaultValue) {
		String rtValue = "";
		Properties propsfile = new Properties();
		File serverfile = new File(filepathname);
		try {
			if (serverfile.exists()) {
				propsfile.load(new FileInputStream(serverfile));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (null == key) {
			LOG.error("key is null");
		} else {
			rtValue = propsfile.getProperty(key);
		}
		if (null == rtValue) {
			LOG.warn("Properties getValues return null, key is " + key);
			rtValue = defaultValue;
		}
		LOG.info("Current Properties getValues: key is " + key + "; Value is " + rtValue);
		return rtValue;
	}
}
