package com.app;

import com.model.Vendor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

public class TesterOrig {

    private static final String topic = "quickstart-VENDOR";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());

        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("group.id","groupId");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topic));

        int counter = 100;

        while (true) {

            try {

                Thread.sleep(1000l);

            } catch (InterruptedException e) {

                e.printStackTrace();

            }

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                Vendor vendor = new Vendor();
                //JSONObject headersObject = new JSONObject();
                JSONObject payload = new JSONObject(record.value().toString());

               /* Headers eventHeaders = record.headers();
                Iterator<Header> iterator = eventHeaders.iterator();
                while (iterator.hasNext()) {
                    Header next = iterator.next();
                    headersObject.put(next.key(), new String(next.value(), StandardCharsets.UTF_8));
                }

                System.out.println("***Headers Object :"+headersObject);*/
                vendor.setId(counter++);

                try{
                    //vendor.setEntityChangeAction(headersObject.getString("entityChangeAction").toString());
                    JSONObject jobj = new JSONObject(payload.get("id").toString());
                    vendor.setEntityChangeAction(payload.get("entityChangeAction").toString());
                    long accid = Long.valueOf(jobj.get("accountId").toString());
                    vendor.setAccountID(accid);
                    vendor.setActive(payload.get("active").toString());
                    vendor.setFullName(payload.get("fullName").toString());
                    System.out.printf("Vendor Class : %s\n", vendor.toString());

                }catch (Exception ex){
                    System.out.println("Uncaught Exception in parsing the JSON Payload  - {}" + ex.getMessage());
                }

                System.out.println("Done!!");

            }

        }
    }
}
