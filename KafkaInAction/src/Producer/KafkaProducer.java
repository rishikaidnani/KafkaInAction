package Producer;

/**
 * Created by rishikaidnani on 9/13/16.
 */
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by idnanir on 9/13/2016.
 */
public class KafkaProducer {

    public static final String topic = "test";
    public Producer<Integer, String> producer;

    public static void main(String args[]) throws Exception {
        KafkaProducer kafkaProducer = new KafkaProducer();
        kafkaProducer.initialize();
        kafkaProducer.publishMessage();
    }

    public void initialize() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(props);
        producer = new Producer<Integer, String>(producerConfig);
    }

    public void publishMessage() throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            System.out.print("Enter message to send to kafka broker(Press 'Y' to close producer): ");
            String msg = null;
            msg = reader.readLine(); // Read message from console

            KeyedMessage<Integer, String> keyedMsg =
                    new KeyedMessage<Integer, String>(topic, msg);

            producer.send(keyedMsg); // This publishes message on given topic
            if ("Y".equals(msg)) {
                break;
            }
            System.out.println("--> Message [" + msg + "] sent.Check message on Consumer 's program console");

        }

        return;

    }


}


