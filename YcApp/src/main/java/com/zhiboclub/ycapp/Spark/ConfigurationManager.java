package com.zhiboclub.ycapp.Spark;

import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
    //私有配置对象
    private static Properties prop = new Properties();

    /**
     * 静态代码块
     */
    static {
        try {
            //获取配置文件输入流
            InputStream in = ConfigurationManager.class
                    .getClassLoader().getResourceAsStream("blaze.properties");

            //加载配置对象
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定key对应的value
     *
     * @param key
     * @return value
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     *
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     *
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取Long类型的配置项
     *
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }
}