package main;

import com.google.common.base.Preconditions;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import config.ConfigProperty;
import db.TomcatJdbcPool;
import infos.PlatInfo;
import infos.TableField;
import infos.TableStruct;
import utils.TimeUtils;

/**
 *
 * 创建人  liangsong
 * 创建时间 2018/11/20 21:21
 */
public class LogDb2ResultDb {
    private static final Logger logger = LoggerFactory.getLogger(LogDb2ResultDb.class);
    public static final DateTimeFormatter MILLIS = DateTimeFormat.forPattern("yyyyMMddHHmmssSSS");
    public static final DateTimeFormatter DAY = DateTimeFormat.forPattern("yyyyMMdd");

    private final ConfigProperty properties;
    private final PlatInfo platInfo;
    private final String configDbName;
    private final String logDbName;
    private final String resultDbName;
    private long startDate;
    private long endDate;
    private final String[] transformTables = new String[]{"createrole", "recharge", "rolelogin", "shop"};
    //, "goodsflow", "moneyflow", "rolelevelup", "rolelogout"

    private TomcatJdbcPool logDbPool;
    private final long currDate = new Date().getTime();
    private final long oneDayMS = TimeUnit.DAYS.toMillis(1);

    public LogDb2ResultDb(String[] args, ConfigProperty properties) throws Exception {
        this.properties = properties;
        long startTime = System.currentTimeMillis();
        if (args.length <= 1) {
            throw new RuntimeException("必需要有游戏名，平台名");
        }
        int count = 0;
        String gameName = args[count++];
        String platName = args[count++];
        platInfo = new PlatInfo(gameName, platName);
        configDbName = "db" + gameName + "conf";
        logDbName = "db" + gameName + platName + "log";
        resultDbName = "db" + gameName + platName + "result";


        if (args.length > count) {

            startDate = getDay(args[count], "开始日期");
        } else {
            startDate = getDay("0", "开始日期");
        }
        count++;
        if (args.length > count) {
            endDate = getDay(args[count], "结束日期");
        } else {
            endDate = startDate + oneDayMS;
        }
        logger.debug("LogDb2ResultDb TimeUtils.printTime(startDate):{}", TimeUtils.printTime(startDate));
        logger.debug("LogDb2ResultDb TimeUtils.printTime(endDate):{}", TimeUtils.printTime(endDate));
        Preconditions.checkArgument(endDate > startDate, "结束日期必需大于等于开始日期startDate:%s, endDate:%s", TimeUtils.printTime(startDate),
                TimeUtils.printTime(endDate));
        startDate /= 1000;
        endDate /= 1000;

        loadPlatInfoFromDb();

        startDBTransform();

    }

    private long getDay(String dayStr, String who) {
        long date = 0;
        Preconditions.checkArgument(dayStr.length() <= 6, "开始%s，必需小于6个字符，可以有负号", who);
        int day = Integer.parseInt(dayStr);
        if (dayStr.length() == 6) {
            Preconditions.checkArgument(day > 20180000 && day < 20380000, "%s果是6位数，取值范围需要 startDay > 20180000&& startDay < 20380000", who);
            date = DAY.parseDateTime(dayStr).getMillis();
        } else {
            Preconditions.checkArgument(day <= 1, "如果%s<6个字符时，必需是<=0", who);
            Date d = new Date(currDate);
            Date d2 = new Date(d.getYear(), d.getMonth(), d.getDate());

            date = d2.getTime() + (day) * oneDayMS;
        }
        return date;
    }

    private void startDBTransform() throws Exception {
        Connection resultDbCon = getJDBCConnection(properties.result_db_url + resultDbName, properties.result_db_user, properties.result_db_passwd);
        Connection logDbCon = getJDBCConnection(properties.log_db_url + logDbName, properties.log_db_user, properties.log_db_passwd);
        HashMap<String, TableStruct> logDbStruct = getTableField(logDbCon);
        checkAddTableIfNotExit(resultDbCon, logDbStruct);


        HashMap<String, TableStruct> resultDbStruct = getTableField(resultDbCon);
        for (String transformTableName : transformTables) {
            Statement resultDbConStatement = resultDbCon.createStatement();
            String conditions =
                    transformTableName + " where unix_timestamp(dtEventTime) > " + startDate + " and unix_timestamp(dtEventTime) <= " + endDate;
            String deleteSql = "delete from " + conditions;
            resultDbConStatement.execute(deleteSql);//删除result库中 表对应时段数据
            resultDbConStatement.close();

            Statement logDbConStatement = logDbCon.createStatement();

            String selectSql = "select * from " + conditions;

            ResultSet logResultSet = logDbConStatement.executeQuery(selectSql);//查log库中 表对应时间的数据


            logger.debug("startDBTransform deleteSql:{}", deleteSql);
            logger.debug("startDBTransform selectSql:{}", selectSql);

            TableStruct tableStruct = resultDbStruct.get(transformTableName);
            String insertSql = tableStruct.getInsertSql();
            logger.debug("startDBTransform -------------------------:{}", transformTableName);
            logger.debug("startDBTransform insertSql:{}", insertSql);
            PreparedStatement preparedStatement = resultDbCon.prepareStatement(insertSql);


            while (logResultSet.next()) {
                //                logger.debug("startDBTransform count:{}", ++count);
                int index = 0;
                for (TableField field : tableStruct.fields) {
                    index++;

                    if (field.fieldName.equals("dt")) {
                        String dtEventTime = logResultSet.getString("dtEventTime");
                        //                        logger.debug("startDBTransform dtEventTime:{}", dtEventTime);
                        String[] split = dtEventTime.split(" ");
                        //                        logger.debug("startDBTransform split[0]:{}", split[0]);
                        preparedStatement.setString(index, split[0]);
                    } else {
                        String string = logResultSet.getString(field.fieldName);
                        //                        logger.debug("startDBTransform string:{}", string);
                        preparedStatement.setString(index, string);


                    }

                }
                preparedStatement.addBatch();

            }
            preparedStatement.executeBatch();

        }

    }

    private HashMap<String, TableStruct> getTableField(Connection logDbCon) throws Exception {

        HashMap<String, TableStruct> ret = new HashMap<>();
        DatabaseMetaData metaData = logDbCon.getMetaData();
        ResultSet tableRet = metaData.getTables(null, "%", "%", new String[]{"TABLE"});
        /*其中"%"就是表示*的意思，也就是任意所有的意思。其中m_TableName就是要获取的数据表的名字，如果想获取所有的表的名字，就可以使用"%"来作为参数了。*/

        //3. 提取表的名字。
        ArrayList<TableField> fields = new ArrayList();
        while (tableRet.next()) {

            String table_name = tableRet.getString("TABLE_NAME");


            String columnName;
            String columnType;
            ResultSet colRet = metaData.getColumns(null, "%", table_name, "%");
            fields.clear();
            while (colRet.next()) {
                columnName = colRet.getString("COLUMN_NAME");
                columnType = colRet.getString("TYPE_NAME");
                int dataSize = colRet.getInt("COLUMN_SIZE");
                //                int nullable = colRet.getInt("NULLABLE");
                String remarks = colRet.getString("REMARKS");


                fields.add(new TableField(columnName, columnType, dataSize, remarks));
            }
            TableField[] tableFields = fields.toArray(TableField.EMPTY);
            TableStruct tableStruct = new TableStruct(table_name, tableFields);
            ret.put(table_name, tableStruct);
        }
        tableRet.close();
        return ret;
    }

    private void checkAddTableIfNotExit(Connection resultDbCon, HashMap<String, TableStruct> tableField) throws SQLException {
        Statement statement = resultDbCon.createStatement();
        for (String transformTable : transformTables) {
            StringBuilder stringBuilder = new StringBuilder("create table if not exists `");

            stringBuilder.append(transformTable);
            stringBuilder.append("` (");
            TableStruct tableStruct = tableField.get(transformTable);
            for (TableField field : tableStruct.fields) {
                stringBuilder.append("\n`");
                stringBuilder.append(field.fieldName);
                stringBuilder.append("` ");

                int size = field.size;
                switch (field.type) {
                    case "varchar":
                        if (size <= 0) {
                            size = 20;
                        }
                        stringBuilder.append("varchar(");
                        stringBuilder.append(size);
                        stringBuilder.append(") ");
                        break;
                    case "int":
                        stringBuilder.append("int(");
                        stringBuilder.append(size);
                        stringBuilder.append(") ");
                        break;
                    case "datetime":
                        stringBuilder.append("datetime ");
                        break;
                    case "bigint":
                        stringBuilder.append("bigint(");
                        stringBuilder.append(size);
                        stringBuilder.append(") ");
                        break;
                    case "tinyint":
                        stringBuilder.append("tinyint(");
                        stringBuilder.append(size);
                        stringBuilder.append(") ");
                        break;
                    default:
                        throw new RuntimeException("未知的数据字段类型:" + field.type);
                }

                stringBuilder.append("COMMENT '");
                stringBuilder.append(field.desc);
                stringBuilder.append("',");
            }
            stringBuilder.append("\n`dt` varchar(30) COMMENT '记录日期'");


            //            stringBuilder.setLength(stringBuilder.length() - 1);
            stringBuilder.append("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;");


            statement.addBatch(stringBuilder.toString());
        }
        statement.executeBatch();
        statement.close();

    }

    private void loadPlatInfoFromDb() throws SQLException {
        Connection connection = getJDBCConnection(properties.result_db_url + configDbName, properties.result_db_user, properties.result_db_passwd);
        assert connection != null;
        try (Statement statement = connection.createStatement()) {
            String sql = "select * from tbplt where vPname = '" + platInfo.getPlatName() + "'";
            ResultSet resultSet = statement.executeQuery(sql);
            int platId = 0;
            if (resultSet.next()) {
                platId = resultSet.getInt("iPid");
            }
            if (platId < 0) {
                throw new RuntimeException(configDbName + "找不到平台配置 platName:" + platInfo.getPlatName());
            } else {
                platInfo.setPlatId(platId);
            }
        } finally {
            connection.close();
        }
    }

    private Connection getJDBCConnection(String url, String user, String password) {

        String driverName = "com.mysql.jdbc.Driver";
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            return DriverManager.getConnection("jdbc:mysql://" + url + "?useUnicode=true&characterEncoding=UTF-8", user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


}
