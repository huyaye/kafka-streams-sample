package com.example.streams;

import com.example.config.HTConstant;
import com.example.interactivequery.InteractiveQueryServer;
import com.example.model.ConnectionEvent;
import com.example.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.example.config.HTConstant.STATE_STORE;

public class ConnectEventStreamsApp {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectEventStreamsApp.class);

    public static void main(String[] args) throws Exception {
        // Host, Port for Interactive query server
        if(args.length < 2){
            LOG.error("Need to specify host, port");
            System.exit(1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        final HostInfo hostInfo = new HostInfo(host, port);

        Properties streamsProperties = getProperties();
        streamsProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host + ":" + port);
        StreamsConfig streamsConfig = new StreamsConfig(streamsProperties);

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, ConnectionEvent> eventKTable = builder.table(HTConstant.SOURCE_TOPIC,
                Materialized.<String, ConnectionEvent, KeyValueStore<Bytes, byte[]>>as(STATE_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(StreamsSerdes.ConnectEventSerde()));
        eventKTable.toStream().print(Printed.<String, ConnectionEvent>toSysOut().withLabel("Event-KTable"));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        InteractiveQueryServer queryServer = new InteractiveQueryServer(kafkaStreams, hostInfo);
        queryServer.init();

        kafkaStreams.setStateListener(((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                LOG.info("Setting the query server to ready");
                queryServer.setReady(true);
            } else if (newState != KafkaStreams.State.RUNNING) {
                LOG.info("State not RUNNING, disabling the query server");
                queryServer.setReady(false);
            }
        }));

        kafkaStreams.setUncaughtExceptionHandler((t, e) -> {
            LOG.error("Thread {} had a fatal error {}", t, e, e);
            shutdown(kafkaStreams, queryServer);
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdown(kafkaStreams, queryServer);
        }));

        LOG.info("Event KTable output started");
//        kafkaStreams.cleanUp();
        kafkaStreams.start();
//        Thread.sleep(600000);
//        LOG.info("Shutting down Event KTable Application now");
//        kafkaStreams.close();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ConnectEvent_app");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ConnectEvent_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "ConnectEvent_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "3000");
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StreamsSerdes.ConnectEventSerde().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

    private static void shutdown(KafkaStreams kafkaStreams, InteractiveQueryServer queryServer) {
        LOG.info("Shutting down the Stock Analysis Interactive Query App Started now");
        kafkaStreams.close();
        queryServer.stop();
    }
}
