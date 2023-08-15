package app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtil;
import utils.DateFormatUtil;
public class BaseLogApp {
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


        //TODO 2.读取kafka topic_db主题数据创建主流
        String topic="topic_db";
        String groupId = "dim_app_230726";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //之后好多程序都要写入kafka中，每次都要写一行代码比较麻烦于是可以创建一个MyKafkaUtil的工具类


        //TODO 3.过滤掉非json数据&将每行数据转化为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){

        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context context, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, value);
                }
            }
        });
        //获取测输出流脏数据并打印
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>>>>>>>>");
        //TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        //TODO 5.使用状态编程做新老访客标记校验
        SingleOutputStreamOperator<JSONObject> jsonObjectWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitState;

            //打op直接出来open函数
            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //获取is_new标记&ts
                String isNew = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                //将时间戳转化为年月日
                String curDt = DateFormatUtil.toDate(ts);//当前日期
                //获取状态中的日期
                String lastDate = lastVisitState.value();//以前日期
                //判断is_new是否为1
                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDt);
                    } else if (!lastDate.equals(curDt)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastDate == null) {
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }
                return value;
            }
        });
        //TODO 6.使用测输出流进行分流处理 页面日志放到主流  启动、曝光、动作、错误放到测输出流
        OutputTag<String> startTag = new OutputTag<>("start");
        OutputTag<String> displayTag = new OutputTag<>("display");
        OutputTag<String> actionTag = new OutputTag<>("action");
        OutputTag<String> errorTag = new OutputTag<>("error");

        SingleOutputStreamOperator<String> pageDS = jsonObjectWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context context, Collector<String> out) throws Exception {
                //尝试获取错误信息
                String err = value.getString("err");
                if (err != null) {
                    context.output(errorTag, value.toJSONString());
                }
                //移除错误信息
                value.remove("err");

                //尝试获取启动信息
                String start = value.getString("start");
                if (start != null) {
                    //将数据写入到start测输出流
                    context.output(startTag, value.toJSONString());
                } else {
                    //获取公共信息&页面id&时间戳
                    String common = value.getString("common");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");

                    //尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("display");
                    if (displays != null && displays.size() > 0) {
                        //遍历曝光数据&写到display测输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            context.output(displayTag, display.toJSONString());
                        }
                    }

                    //尝试获取曝光数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        //遍历曝光数据&写到display测输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", pageId);
                            action.put("ts", ts);
                            context.output(displayTag, action.toJSONString());
                        }
                    }

                    //移除曝光和动作数据&写到页面日志主流
                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());
                }
            }
        });
        //TODO 7.提取各个侧输出流数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        //TODO 8.将数据打印并写入对应主题


        pageDS.print("Page>>>>>>>>>>>>>>>");
        pageDS.print("Start>>>>>>>>>>>>>>>");
        pageDS.print("Display>>>>>>>>>>>>>>>");
        pageDS.print("Action>>>>>>>>>>>>>>>");
        pageDS.print("Error>>>>>>>>>>>>>>>");


        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getKafkaProducer(error_topic));
        //TODO 9.启动任务
        env.execute();
    }


}
