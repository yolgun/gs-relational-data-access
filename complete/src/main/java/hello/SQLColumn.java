package hello;

/**
 * Created by yunus on 12.12.16.
 */
public class SQLColumn {
    private String name;
    private boolean autoIncrement;
    private String type;
    private int typeCode;
    private String tableName;

    public void setName(String name) {
        this.name = name;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setTypeCode(int typeCode) {
        this.typeCode = typeCode;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "SQLColumn{" +
                "name='" + name + '\'' +
                ", autoIncrement=" + autoIncrement +
                ", type='" + type + '\'' +
                ", typeCode=" + typeCode +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
