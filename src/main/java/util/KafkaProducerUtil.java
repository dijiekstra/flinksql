package util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class KafkaProducerUtil extends Thread {

    private String topic;


    public KafkaProducerUtil(String topic) {
        super();
        this.topic = topic;
    }

    private Producer<String, String> createProducer() {
        // 通过Properties类设置Producer的属性
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(properties);
    }

    @Override
    public void run() {
        Producer<String, String> producer = createProducer();
        Random random = new Random();
        Random random2 = new Random();

        while (true) {
            int nums = random.nextInt(10);
            int nums2 = random2.nextInt(10);

            String time = new Date().getTime() / 1000 + 5 + "";
            String type = "pv";
            try {
                if (nums2 % 2 == 0) {
                    type = "pv";
                } else {
                    type = "uv";

                }
                String kaifa_log = "{\"user_id\":" + nums + ",\"item_id\":" + nums * 10 + ",\"category_id\":" + nums2 + ",\"behavior\":\"" + type + "\",\"ts\":\"" + time + "\"}";
                System.out.println("kaifa_log = " + kaifa_log);
                producer.send(new ProducerRecord<String, String>(this.topic, kaifa_log));


            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("=========循环一次==========");


            try {
                sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new KafkaProducerUtil("08_test").run();

    }

}
