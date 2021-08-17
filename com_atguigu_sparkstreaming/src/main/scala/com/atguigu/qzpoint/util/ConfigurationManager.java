package com.atguigu.qzpoint.util;


import java.io.InputStream;
import java.util.Properties;

/**
 * 读取配置文件工具类
 */
public class ConfigurationManager {
    private static Properties prop = new Properties();

    // 使用静态代码块 可以随着类的加载而加载，这样能在类加载之时，将properties文件加载进来
    static {
        try {
            // 使用反射将文件读取为InputStream对象
            InputStream inputStream = ConfigurationManager.class.getClassLoader().getResourceAsStream("comerce.properties");
            // 从inputStream中加载配置信息
            prop.load(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取key所对应的配置项信息
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    //获取布尔类型的配置项 （项目中未用到）
    public static boolean getBoolean(String key) {
        String value = prop.getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
