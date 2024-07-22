package demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());



    public static void main(String[] args) throws InterruptedException {

        log.info("I am a Kafka producer");

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

        String topic = "wikimedia.recent.changes";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";


        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();


        // start the producer in another thread
        eventSource.start();


        // we produce for 10 minutes and block the program until the
        TimeUnit.MINUTES.sleep(10);

    }
}

