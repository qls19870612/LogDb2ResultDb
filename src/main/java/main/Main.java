package main;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import config.ConfigProperty;

/**
 *
 * 创建人  liangsong
 * 创建时间 2018/11/20 20:24
 */
public class Main {
    /**
     * 平台ID 截止时间
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.load(new FileInputStream(new File("log2ResultConfiguration.property")));
        ConfigProperty configProperty = new ConfigProperty(properties);
        new LogDb2ResultDb(args, configProperty);

    }
}
