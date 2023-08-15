package bean;

import lombok.Data;

@Data
public class TableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;

    public String getSourceTable() {
        return sourceTable;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public String getSinkColumns() {
        return sinkColumns;
    }

    public String getSinkPk() {
        return sinkPk;
    }

    public String getSinkExtend() {
        return sinkExtend;
    }

    //建表扩展
    String sinkExtend;
}
