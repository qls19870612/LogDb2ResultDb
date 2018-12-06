package infos;

/**
 *
 * 创建人  liangsong
 * 创建时间 2018/11/21 11:15
 */
public class TableField {
    public static final TableField[] EMPTY = new TableField[0];
    public final String fieldName;
    public final String type;
    public final int size;
    public final String desc;

    public TableField(String fieldName, String type, int size, String desc) {
        this.fieldName = fieldName;
        this.type = type.toLowerCase();
        this.size = size;
        this.desc = desc;
    }
}
