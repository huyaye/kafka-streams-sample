package com.example.interactivequery;

import com.example.model.ConnectionEvent;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import java.util.*;

import static spark.Spark.get;
import static spark.Spark.port;

public class InteractiveQueryServer {
    private static final Logger LOG = LoggerFactory.getLogger(InteractiveQueryServer.class);
    private final Gson gson = new Gson();
    private final KafkaStreams kafkaStreams;
    private Client client = ClientBuilder.newClient();
    private final HostInfo hostInfo;
    private static final String STORE_PARAM = ":store";
    private static final String KEY_PARAM = ":key";
    private static final String FROM_PARAM = ":from";
    private static final String TO_PARAM = ":to";
    private StringSerializer stringSerializer = new StringSerializer();
    private volatile boolean ready = false;
    private static final String STORES_NOT_ACCESSIBLE = "{\"message\":\"Stores not ready for service, probably re-balancing\"}";


    public InteractiveQueryServer(final KafkaStreams kafkaStreams, final HostInfo hostInfo) {
        this.kafkaStreams = kafkaStreams;
        this.hostInfo = hostInfo;
        port(hostInfo.port());
    }

    public void init() {
        LOG.info("Started the Interactive Query Web server");
        get("/kv/:store", (req, res) -> ready ? fetchAllFromKeyValueStore(req.params()) : STORES_NOT_ACCESSIBLE);
        get("/kv/:store/:key", (req, res) -> ready ? fetchFromKeyValueStore(req.params()) : STORES_NOT_ACCESSIBLE);
        // this is a special URL meant only for internal purposes
        get("/local/kv/:store", (req, res) -> ready ? fetchAllFromLocalKeyValueStore(req.params()) : STORES_NOT_ACCESSIBLE);
    }


    private String fetchAllFromLocalKeyValueStore(Map<String, String> params) {
        String store = params.get(STORE_PARAM);
        Collection<StreamsMetadata> metadata = kafkaStreams.allMetadataForStore(store);
        for (StreamsMetadata streamsMetadata : metadata) {
            if (localData(streamsMetadata.hostInfo())) {
                return getKeyValuesAsJson(store);
            }
        }
        return "[]";
    }

    @SuppressWarnings("unchecked")
    private String fetchAllFromKeyValueStore(Map<String, String> params) {
        String store = params.get(STORE_PARAM);
        List<KeyValue<String, ConnectionEvent>> allResults = gson.fromJson(getKeyValuesAsJson(store), List.class);
        Collection<StreamsMetadata> streamsMetadata = kafkaStreams.allMetadataForStore(store);
        for (StreamsMetadata streamsMetadatum : streamsMetadata) {
            if (dataNotLocal(streamsMetadatum.hostInfo())) {
                List<KeyValue<String, ConnectionEvent>> remoteResults = gson.fromJson(fetchRemote(streamsMetadatum.hostInfo(), "local/kv", params), List.class);
                allResults.addAll(remoteResults);
            }
        }
        return gson.toJson(new HashSet<>(allResults));
    }

    @SuppressWarnings("unchecked")
    private String fetchFromKeyValueStore(Map<String, String> params) {
        String store = params.get(STORE_PARAM);
        String key = params.get(KEY_PARAM);

        HostInfo storeHostInfo = getHostInfo(store, key);
        if (storeHostInfo.host().equals("unknown")) {
            return STORES_NOT_ACCESSIBLE;
        }
        if (dataNotLocal(storeHostInfo)) {
            LOG.info("{} located in state store in another instance", key);
            return fetchRemote(storeHostInfo, "kv", params);
        }
        String value = getKeyValuesAsJson(store, key);
        return value;
    }

    private String getKeyValuesAsJson(String store) {
        ReadOnlyKeyValueStore<String, ConnectionEvent> readOnlyKeyValueStore = kafkaStreams.store(store, QueryableStoreTypes.keyValueStore());
        List<KeyValue<String, ConnectionEvent>> keyValues = new ArrayList<>();
        try (KeyValueIterator<String, ConnectionEvent> iterator = readOnlyKeyValueStore.all()) {
            while (iterator.hasNext()) {
                keyValues.add(iterator.next());
            }
        }
        return gson.toJson(keyValues);
    }

    private String getKeyValuesAsJson(String store, String key) {
        ReadOnlyKeyValueStore<String, ConnectionEvent> readOnlyKeyValueStore = kafkaStreams.store(store, QueryableStoreTypes.keyValueStore());
        ConnectionEvent connectionEvent = readOnlyKeyValueStore.get(key);
        return gson.toJson(connectionEvent);
    }

    private String fetchRemote(HostInfo hostInfo, String path, Map<String, String> params) {
        String store = params.get(STORE_PARAM);
        String key = params.get(KEY_PARAM);
        String from = params.get(FROM_PARAM);
        String to = params.get(TO_PARAM);

        String url;

        if (from != null && to != null) {
            url = String.format("http://%s:%d/%s/%s/%s/%s/%s", hostInfo.host(), hostInfo.port(), path, store, key, from, to);
        } else if (key != null) {
            url = String.format("http://%s:%d/%s/%s/%s", hostInfo.host(), hostInfo.port(), path, store, key);
        } else {
            url = String.format("http://%s:%d/%s/%s", hostInfo.host(), hostInfo.port(), path, store);
        }

        String remoteResponseValue = "";

        try {
            remoteResponseValue = client.target(url).request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
        } catch (Exception e) {
            LOG.error("Problem connecting " + e.getMessage());
        }
        return remoteResponseValue;
    }

    private HostInfo getHostInfo(String storeName, String key) {
        StreamsMetadata metadata = kafkaStreams.metadataForKey(storeName, key, stringSerializer);
        return metadata.hostInfo();
    }

    private boolean dataNotLocal(HostInfo hostInfo) {
        return !this.hostInfo.equals(hostInfo);
    }

    private boolean localData(HostInfo hostInfo) {
        return !dataNotLocal(hostInfo);
    }

    public synchronized void setReady(boolean ready) {
        this.ready = ready;
    }

    public void stop() {
        Spark.stop();
        client.close();
        LOG.info("Shutting down the Interactive Query Web server");
    }

}
