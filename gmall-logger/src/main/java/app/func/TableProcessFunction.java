package app.func;

import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_DRIVER);
    }

    @Override//处理广播流---写状态
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //定义MapStateDescriptor状态描述器时定义的输入输出类型为：<String, TableProcess>
        //1.将String解析成TableProcess（获取并解析数据，方便主流操作）
        JSONObject jsonObject = JSON.parseObject(value);//需要知道你处理的数据长什么样子
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);
        //2.检验表是否存在，若不存在则需在Phoenix中建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());

        //3.写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(),tableProcess);

    }
    //2.
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
            //处理特殊字段
            if (sinkPk == null || "".equals(sinkPk)) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            //拼接SQL
            StringBuilder createTableSql = new StringBuilder("create table if not exists")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                //取出字段
                String column = columns[i];
                //判断是否为主键
                if (sinkPk.equals(column)) {
                    createTableSql.append(column).append(" varchar primary key");
                } else {
                    createTableSql.append(column).append(" varchar");
                }
                //判断是否为最后一个字段
                if (i < columns.length - 1) {
                    createTableSql.append(",");
                }
            }
            createTableSql.append(")").append(sinkExtend);
            //编译SQL
            System.out.println("建表语句为：" + createTableSql);
            preparedStatement = connection.prepareStatement(createTableSql.toString());
            //执行SQL，建表
            preparedStatement.execute();

        } catch (SQLException throwables) {
            throw new RuntimeException("建表失败：" + sinkTable);//把编译时异常改为运行时异常,出现异常程序可以停止执行
        } finally {
            //释放资源
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        //建表失败，应该把这个异常捕获到，直接处理让程序继续运行；还是直接让程序停掉？
        //建表失败还往主流数据里面写？没有表写什么？
        //所以需要把程序停掉
    }

    //主流数据从哪来---topic_db,
    //topic_db是谁写进去的？
    @Override//处理主流---读状态
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.获取广播流的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String table=value.getString("table");//能从广播状态拿到table就能拿到tableprocess
        TableProcess tableProcess = broadcastState.get(table);//tableProcess有没有可能为null？如果有可能在什么时候为nul值？
        if(tableProcess!=null){//过滤数据的核心
            //2.过滤字段filterColumn
            filterColumn(value.getJSONObject("data"),tableProcess.getSinkColumns());//如果为null就直接调用其tableProcess空指针了
            //3.补充SinkTable字段并写出到流中
            value.put("sinkTable",tableProcess.getSinkTable());
            out.collect(value);
        }else{
            System.out.println("找不到对应的Key:"+table);
        }


    }
    //过滤字段116行
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] columns=sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

//        Set<Map.Entry<String, Object>> entries = data.entrySet();//当JSONObject为map
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();//java集合遍历
//        while(iterator.hasNext()){
//            Map.Entry<String, Object> next = iterator.next();
//            if (!sinkColumns.contains(next.getKey())){//列名之间可能会有包含关系，比如“name"和”tm_name“
//                //直接用字符串判断!sinkColumns.contains，可能会出现
//                iterator.remove();
//            }
//        }
        //上面的简写版
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !sinkColumns.contains(next.getKey()));
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
