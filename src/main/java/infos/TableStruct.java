package infos;

import java.util.HashMap;

/**
 *
 * 创建人  liangsong
 * 创建时间 2018/11/21 14:40
 */
public class TableStruct {
    public String tableName;
    public final TableField[] fields;
    private String ret;

    public HashMap<String, TableField> getFiledMap() {
        if (filedMap == null) {
            filedMap = new HashMap<>();
            for (TableField field : fields) {
                filedMap.put(field.fieldName, field);
            }
        }
        return filedMap;
    }

    public boolean hasField(String fileName) {
        return getFiledMap().containsKey(fileName);
    }

    private HashMap<String, TableField> filedMap;

    public TableStruct(String tableName, TableField[] fields) {
        this.tableName = tableName;
        this.fields = fields;
    }

    public String getInsertSql() {
        if (ret == null) {
            StringBuilder builder = new StringBuilder();
            builder.append("insert into ");
            builder.append(tableName);
            builder.append(" (");
            for (TableField field : fields) {
                builder.append(field.fieldName);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
            builder.append(") values (");
            for (TableField field : fields) {
                builder.append("?,");
            }
            builder.setLength(builder.length() - 1);
            builder.append(")");
            ret = builder.toString();

        }
        return ret;
    }

}
