package config;

import java.util.Properties;

/**
 *
 * 创建人  liangsong
 * 创建时间 2018/11/20 18:10
 */
public class ConfigProperty extends Properties {
    //log db
    public final String log_db_url;
    public final String log_db_user;
    public final String log_db_passwd;
    //result db
    public final String result_db_url;
    public final String result_db_passwd;
    public final String result_db_user;

    public ConfigProperty(Properties defaults) {
        super(defaults);

        log_db_url = getString("log_db_url");
        log_db_user = getString("log_db_user");
        log_db_passwd = getString("log_db_passwd");
        result_db_url = getString("result_db_url");
        result_db_user = getString("result_db_user");
        result_db_passwd = getString("result_db_passwd");

    }

    private String getString(String key) {
        return (String) defaults.get(key);
    }

    private boolean getBoolean(String key) {
        return getString(key).equals("1");
    }
}
