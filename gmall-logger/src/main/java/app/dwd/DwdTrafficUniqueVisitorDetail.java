package app.dwd;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import utils.DateFormatUtil;
import utils.MyKafkaUtil;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
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
        //TODO 2.获取kafka 页面日志主题创建流
        String topic = "dwd_traffic_page_log";
        String groupID="Unique_Visitor_Detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupID));
        //TODO 3.过滤掉上一跳页面不为null的数据并将每行数据转换我为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(value);
                    //获取上一跳页面
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(value);//快捷键sout
                }
            }
        });
        //TODO 4.按照MID分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        //TODO 5。使用状态编程实现按照mid去重
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<String>("last_visit", String.class);
                lastVisitState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //获取状态数据&当前数据中的时间戳并转换为日期
                String lastDate = lastVisitState.value();
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);
                if (lastDate == null || !lastDate.equals(curDate)) {
                    lastVisitState.update(curDate);
                    return true;
                } else {
                    return false;
                }

            }
        });
        //TODO 6.将数据写到Kafka
        String targetTopic="dwd_traffic_unique_visitor_detail";
        uvDS.print(">>>>>>>>>>>>>");
        uvDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(targetTopic));
        //TODO 7.启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}
