import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;


/**
 * Created by TUSHAR_SK on 11/5/16.
 */

public class consumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "testgroup");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("firstTopic","secondTopic"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(2000000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = "+ record.topic() + " partition = "+ record.partition()+ ", offset = " +record.offset()+", ip = "+ record.key()+ ", message = "+ record.value());

                }
            }
        }finally {
            consumer.close();
        }
    }

}