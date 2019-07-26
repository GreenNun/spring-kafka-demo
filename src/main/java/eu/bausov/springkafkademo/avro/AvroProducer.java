package eu.bausov.springkafkademo.avro;

import eu.bausov.springkafkademo.avro.model.Employee;
import eu.bausov.springkafkademo.avro.model.PhoneNumber;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;
import java.util.stream.LongStream;

/**
 * Created by Stanislav Bausov on 26.07.2019.
 */
public class AvroProducer {
    private final static String TOPIC = "new-employees";

    private static Producer<Long, Employee> createProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return new KafkaProducer<>(props);
    }

    public static void main(String... args) {
        final Producer<Long, Employee> producer = createProducer();
        final Employee bob = Employee.newBuilder()
                .setAge(35)
                .setFirstName("Bob")
                .setLastName("Jones")
                .setPhoneNumber(
                        PhoneNumber.newBuilder()
                                .setAreaCode("301")
                                .setCountryCode("1")
                                .setPrefix("555")
                                .setNumber("1234")
                                .build())
                .build();

        LongStream.range(1, 100).forEach(index -> {
            producer.send(new ProducerRecord<>(TOPIC, index, bob));
        });

        producer.flush();
        producer.close();
    }
}
