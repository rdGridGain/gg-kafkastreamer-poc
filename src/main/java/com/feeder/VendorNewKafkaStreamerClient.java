package com.feeder;

import com.model.Vendor;
import org.apache.ignite.*;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.stream.kafka.KafkaStreamer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

public class VendorNewKafkaStreamerClient implements LifecycleBean {
    static final String VENDOR_CACHE_NAME = "VENDOR_SOURCE_CACHE";
    static final String TOPIC = "quickstart-VENDOR";
    static final String BOOTSTRAP_SERVERS =  "localhost:9092";

    @IgniteInstanceResource
    private Ignite ignite;
    @LoggerResource
    protected IgniteLogger log;
    private IgniteCache<Integer, Vendor> vendorCache;
    private IgniteDataStreamer<Integer, Vendor> streamer;
    private KafkaStreamer<Integer, Vendor> kafkaStreamer;

    //private static final Log log = LogFactory.getLog(VendorNewKafkaStreamerClient.class);

    //private static final AtomicInteger CNT = new AtomicInteger();
    private static final AtomicInteger INTKEY = new AtomicInteger();

    public static void main(String[] args) throws IgniteException {
        Ignite igniteInstance = Ignition.start("client-config.xml");
    }
  
    @Override
    public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
        switch (evt) {
            case AFTER_NODE_START:
                onStart();
                break;
            case BEFORE_NODE_STOP:
                onShutdown();
                break;
        }
    }

    public void onStart() {
        //PU.log("On start");
        vendorCache = ignite.getOrCreateCache(VENDOR_CACHE_NAME);
        streamer = ignite.dataStreamer(VENDOR_CACHE_NAME);
        streamer.allowOverwrite(true);
        streamer.autoFlushFrequency(10);

        // Configure Kafka streamer.
        kafkaStreamer = new KafkaStreamer<>();

        // Get the cache.
        //IgniteCache<Integer, Vendor> cache = ignite.cache(VENDOR_CACHE_NAME);

        // Set Ignite instance.
        kafkaStreamer.setIgnite(ignite);

        // Set data streamer instance.
        kafkaStreamer.setStreamer(streamer);

        // Set the topic.
        kafkaStreamer.setTopic(Arrays.asList(TOPIC));

        // Set the number of threads.
        kafkaStreamer.setThreads(4);

        // Set the consumer configuration.
        kafkaStreamer.setConsumerConfig(
                createDefaultConsumerConfig(BOOTSTRAP_SERVERS, "groupId"));

        kafkaStreamer.setMultipleTupleExtractor(
                record -> {
                    Map<Integer, Vendor> entries = new HashMap<>();

                    try {
                        Vendor vendor = new Vendor();
                        vendor.setId(INTKEY.getAndIncrement());

                        // Convert the message into number of cache entries with same key or dynamic key from actual message.
                        // For now using key as cache entry key and value as cache entry value - for test purpose.

                        JSONObject headersObject = new JSONObject();
                        JSONObject payload = new JSONObject(record.value().toString());

                        Headers eventHeaders =record.headers();
                        Iterator<Header> iterator = eventHeaders.iterator();
                        while (iterator.hasNext()) {
                            Header next = iterator.next();
                            headersObject.put(next.key(), new String(next.value(), StandardCharsets.UTF_8));
                        }

                        try{
                            JSONObject jobj = new JSONObject(payload.get("id").toString());
                            vendor.setEntityChangeAction(payload.get("entityChangeAction").toString());
                            long accid = Long.valueOf(jobj.get("accountId").toString());
                            vendor.setAccountID(accid);
                            vendor.setActive(payload.get("active").toString());
                            vendor.setFullName(payload.get("fullName").toString());
                            System.out.printf("Vendor Class : %s\n", vendor.toString());

                        }catch (Exception ex){
                            log.error("Uncaught Exception in parsing the JSON Payload  - {}" + ex.getMessage());
                        }

                        entries.put(vendor.getId(),vendor);
                        System.out.printf("*******************Cached*********************\n ");
                    }
                    catch (Exception ex) {
                        log.info("***Unexpected error." + ex);
                    }

                    return entries;
                });

        //final CountDownLatch latch = new CountDownLatch(CNT);

        IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
            @IgniteInstanceResource
            private Ignite ig;

            @LoggerResource
            private IgniteLogger log;

            /** {@inheritDoc} */
            @Override public boolean apply(UUID uuid, CacheEvent evt) {
                //latch.countDown();
                System.out.printf("*******************Cache Event -1 *********************\n ");
                if (log.isInfoEnabled()) {
                    IgniteEx igEx = (IgniteEx)ig;

                    UUID nodeId = igEx.localNode().id();

                    log.info("Received event=" + evt + ", nodeId=" + nodeId);
                }
                System.out.printf("*******************Cache Event - complete*********************\n ");
                return true;
            }
        };
        ignite.events(ignite.cluster().forCacheNodes(VENDOR_CACHE_NAME)).remoteListen(locLsnr, null, EVT_CACHE_OBJECT_PUT);

        // Start kafka streamer.
        System.out.printf("*******************Ready to start Kafka streamer *********************\n ");

        kafkaStreamer.start();
        System.out.printf("*******************Kafka stream started *********************\n ");
    }


    public void onShutdown() {
        //PU.log("On stop");
        System.out.printf("Stopping the server");
        kafkaStreamer.stop();
        streamer.close();
        System.out.printf("Closing the connection");
    }
    /**
     * Creates default consumer config.
     *
     * @param servers Bootstrap servers' address in the form of &lt;server:port;server:port&gt;.
     * @param grpId Group Id for kafka subscriber.
     * @return Kafka consumer configuration.
     */
    private static Properties createDefaultConsumerConfig(String servers, String grpId) {
        A.notNull(servers, "bootstrap servers");
        A.notNull(grpId, "groupId");

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", servers);
        props.setProperty("group.id", grpId);
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("auto.offset.reset","earliest");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }
}
