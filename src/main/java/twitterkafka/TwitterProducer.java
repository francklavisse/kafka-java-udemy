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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.cdimascio.dotenv.Dotenv;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    Dotenv dotenv = Dotenv.load();

    public TwitterProducer(){}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("--- SETUP ---");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Client client = createTwitterClient(msgQueue);
        client.connect();

//        KafkaProducer<String, String> producer = createKafkaProducer();

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
            }
        }
        logger.info("--- EXIT ---");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("twitter", "api");
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
}
