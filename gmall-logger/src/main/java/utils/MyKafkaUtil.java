package utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class MyKafkaUtil {

    private static final String KAFKA_SERVER="hadoop102:9092";
    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String groupId){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        return new FlinkKafkaConsumer<String>(
                topic,
                //new SimpleStringSchema(),//反序列化方法不能为null，如果为null会抛出空指针异常，所以需要自定义一个
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {//是否是流的最后一个
                        return false;//如果是无界流，返回false
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {//最核心的方法
                        //如果过来的数据为null
                        if(consumerRecord==null||consumerRecord.value()==null){
                            return null;//不会抛空指针异常
                        }else{
                            return new String(consumerRecord.value());
                        }
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                },
                properties
        );
    }
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>(KAFKA_SERVER,topic,new SimpleStringSchema());
    }
//    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
//
//        Properties prop = new Properties();
//        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
//        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60 * 15 * 1000 + "");
//        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(new KafkaSerializationSchema<String>() {
//
//            @Override
//            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
//                if(element==null){
//                    return null;
//                }
//                return new ProducerRecord<byte[], byte[]>(topic, element.getBytes());
//            }
//        }, prop,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
//        return producer;
//    }
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic,String defaultTopic){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        return new FlinkKafkaProducer<String>(defaultTopic, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                if(element==null){
                    return new ProducerRecord<>(topic,"".getBytes());
                }
                return new ProducerRecord<>(topic,element.getBytes());
            }
        },properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);


    }

}
