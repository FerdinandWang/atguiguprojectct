package com.atguigu.ct.consumer.bean;

import com.atguigu.ct.common.bean.Consumer;
import com.atguigu.ct.common.constant.Names;
import kafka.Kafka;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志的消费者对象
 */
public class CalllogConsumer implements Consumer {
    /**
     * 消费数据
     */
    public void consume() {
        //创建配置对象
        Properties properties = new Properties();



        //获取flume采集的数据
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //关注主题
        consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));

        //消费数据
        while(true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.value());
            }
        }

    }

    /**
     * 关闭资源
     * @throws IOException
     */
    public void close() throws IOException {

    }
}
