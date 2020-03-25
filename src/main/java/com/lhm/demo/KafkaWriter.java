package com.lhm.demo;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaWriter {

    //本地的kafka机器列表
    public static final String BROKER_LIST = "172.29.167.169:9092";
    //kafka的topic
    public static final String TOPIC_NAME = "test";
    //key序列化的方式，采用字符串的形式
    public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    //value的序列化的方式
    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static void writeToKafka() throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", KEY_SERIALIZER);
        props.put("value.serializer", VALUE_SERIALIZER);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //构建Person对象，在name为kafka后边加个随机数
        int randomInt = RandomUtils.nextInt(1, 100000);
        person person = new person();
        person.setId(randomInt);
        person.setName("kafka" + randomInt);
        person.setAge(randomInt);
        person.setCreateDate(new Date());
        //转换成JSON
        String personJson = JSON.toJSONString(person);

        //包装成kafka发送的记录
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, null,
                null, personJson);
        //发送到缓存
        producer.send(record);
        System.out.println("向kafka发送数据:" + personJson);
        //立即发送
        producer.flush();

    }

    public static void main(String[] args) {
        while(true) {
            try {
                //每三秒写一条数据
                TimeUnit.SECONDS.sleep(3);
                writeToKafka();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

}
