package com.lhm.demo;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Properties;

public class FlinkConsumeKafka {

    public static void main(String[] args) throws Exception{
        //构建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 非常关键，一定要设置启动检查点！！
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //kafka
        Properties prop = new Properties();
        prop.put("bootstrap.servers", KafkaWriter.BROKER_LIST);
        prop.put("group.id", KafkaWriter.TOPIC_NAME);
        prop.put("key.serializer", KafkaWriter.KEY_SERIALIZER);
        prop.put("value.serializer", KafkaWriter.VALUE_SERIALIZER);
        prop.put("auto.offset.reset", "latest");

        FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<String>(
                KafkaWriter.TOPIC_NAME,
                new SimpleStringSchema(),
                prop
        );
        //单线程打印，控制台不乱序，不影响结果
        DataStream<String> dataStreamSource = env.addSource(source). setParallelism(1);

        //从kafka里读取数据，转换成Person对象
        DataStream<person> dataStream = dataStreamSource.map(value -> JSONObject.parseObject(value, person.class));
        //收集5秒钟的总数
        dataStream.timeWindowAll(Time.seconds(5L)).
                apply(new AllWindowFunction<person, List<person>, TimeWindow>() {

                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<person> iterable, Collector<List<person>> out) throws Exception {
                        List<person> persons = Lists.newArrayList(iterable);

                        if(persons.size() > 0) {
                            System.out.println("5秒的总共收到的条数：" + persons.size());
                            out.collect(persons);
                        }

                    }
                })
                //sink 到数据库
                .addSink(new MysqlSink());
        //打印到控制台
        //.print();


        env.execute("kafka 消费任务开始");
    }

}
