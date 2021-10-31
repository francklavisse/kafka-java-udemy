package twitterkafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.cdimascio.dotenv.Dotenv;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterOptimizedProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    Dotenv dotenv = Dotenv.load();

    public TwitterOptimizedProducer(){}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("--- SETUP ---");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Client client = createTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String, String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            client.stop();
            producer.close();
            logger.info("--- KAFKA CLOSED ---");
        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("ERROR", e);
                        }
                    }
                });
            }
        }
        logger.info("--- EXIT ---");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("bitcoin", "eth", "btc");
        endpoint.followings(followings);
        endpoint.trackTerms(terms);

        Authentication oauth = new OAuth1(
                dotenv.get("CONSUMER_KEY"),
                dotenv.get("CONSUMER_SECRET"),
                dotenv.get("TOKEN"),
                dotenv.get("SECRET"));

        return new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosts)
                .authentication(oauth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue)).build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";

        // basic settings
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // make it safe
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // optimization
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        return new KafkaProducer<String, String>(properties);
    }
}
