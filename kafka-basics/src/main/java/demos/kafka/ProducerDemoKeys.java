package demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka producer with callback");

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "https://internal-deer-9056-us1-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"aW50ZXJuYWwtZGVlci05MDU2JINR_H5miMmDUrsVz2w45OSa7VYbJ-QGgAPyFLI\" password=\"YjY1ZDAyMGQtMDBhZi00ZmVjLWI2NDYtZjE0NWQzYWM5YjRi\";");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 30; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world" + i;

                // create a Producer Record
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(record, (recordMetadata, e) -> {
                    // executes every time a record successfully sent or an exception is thrown
                    if(e == null){
                        log.info("Received new metadata Key:{}\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}\n", key, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    }else {
                        log.error("Error while sending record", e);
                    }

                });
            }
        }


        // tell the producer to send all data and block until done - synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
