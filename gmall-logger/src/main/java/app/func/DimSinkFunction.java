package app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import utils.DruidDSUtil;
import utils.PhoenixUtil;

import java.sql.SQLException;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建连接池
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //1.获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();
        //2.写出数据
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        PhoenixUtil.upsertValues(connection, sinkTable, data);//三个参数分别是：连接、表名、数据源
        //3.归还连接
        connection.close();
    }
}
