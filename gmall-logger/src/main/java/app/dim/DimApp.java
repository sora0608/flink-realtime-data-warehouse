package app.dim;

import app.func.DimSinkFunction;
import app.func.TableProcessFunction;
import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.util.Collector;
import utils.MyKafkaUtil;

public class DimApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境中设置为kafka主题的分区数

//        //1.1开启checkpoint（检查点）
//        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);//设置为5min一次
//        env.getCheckpointConfig().setCheckpointTimeout(10*60000L);//超时
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);//最大并发检查点的数量
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));//重启策略
//        //1.2设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/230726/ck");//设置检查点的存储路径
//        System.setProperty("HADOOP_USER_NAME","atguigu");//设置Hadoop用户名，以便在使用Hadoop作为检查点存储路径时进行认证


        //2.读取kafka topic_db主题数据创建主流
        String topic="topic_db";
        String groupId = "dim_app_230726";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //之后好多程序都要写入kafka中，每次都要写一行代码比较麻烦于是可以创建一个MyKafkaUtil的工具类


        //3.过滤掉非json数据以及保留新增、变化以及初始化数据并将
        SingleOutputStreamOperator<JSONObject> filterJsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    //将数据转换为JSON格式
                    JSONObject jsonObject = JSON.parseObject(value);
                    //获取数据中的操作类型字段
                    String type = jsonObject.getString("type");
                    //保留新增、变化以及初始化数据
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        out.collect(jsonObject);
                    }

                } catch (Exception e) {
                    System.out.println("发现脏数据：" + value);
                }

            }
        });
        //4.使用flinkCDC读取MYSQL配置信息表创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.table_process") // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        // 封装为流
        DataStreamSource<String> mysqlDSSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");


        //5.将配置流处理为广播流
        MapStateDescriptor<String, TableProcess> MapStateDescriptor = new MapStateDescriptor<String, TableProcess>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDSSource.broadcast(MapStateDescriptor);

        //6.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterJsonObjDS.connect(broadcastDS);//得到一个连接流
        //7.处理连接流，根据配置信息处理主流数据,得到的结果类型还是主流类型
        SingleOutputStreamOperator<JSONObject> dimDS = connectedStream.process(new TableProcessFunction(MapStateDescriptor));
        //8.将数据写出到Phoenix
        dimDS.print(">>>>>>>>>");
        dimDS.addSink(new DimSinkFunction());
        //9.启动任务
        env.execute("DimApp");
    }
}
