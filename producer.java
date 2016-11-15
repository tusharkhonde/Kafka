/**
 * Created by TUSHAR_SK on 11/5/16.
 */


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;


public class producer {

    private static Producer<String, String> producer;

   private producer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "testgroup");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

    public static void main(String[] args) {
        long events = 5;
        Random rnd = new Random();
        new producer();
        for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = new Date().getTime();
            // For firstTopic
            String ip1 = "192.168.1." + rnd.nextInt(255);
            String msg1 = runtime + ",www.firstTopic.com," + ip1;
            // For secondTopic
            String ip2 = "192.168.2." + rnd.nextInt(255);
            String msg2 = runtime + ",www.secondTopic.com," + ip2;

            ProducerRecord<String, String> data1 = new ProducerRecord<String, String>("firstTopic",ip1, msg1);
            producer.send(data1);
            
            ProducerRecord<String, String> data2 = new ProducerRecord<String, String>("secondTopic",ip2, msg2);
            producer.send(data2);
        }
        producer.close();
    }
}
