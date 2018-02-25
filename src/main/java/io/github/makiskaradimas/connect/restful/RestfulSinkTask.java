package io.github.makiskaradimas.connect.restful;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.github.makiskaradimas.dataenrichment.URIProvider;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class RestfulSinkTask extends SinkTask {
    private final static Logger LOGGER = LoggerFactory.getLogger(RestfulSinkTask.class);

    private RestfulSinkConnectorConfig restfulSinkConnectorConfig;

    @Override
    public String version() {
        return new RestfulSinkConnector().version();
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        this.onPartitionsAssigned(partitions);
        LOGGER.info("Calling open");
        partitions.forEach(partition -> LOGGER.info("Assigned partition " + partition.toString()));
    }

    @Override
    public void start(Map<String, String> map) {
        LOGGER.info("Starting Restful sink task");

        restfulSinkConnectorConfig = RestfulSinkConnectorConfig.getRestFulSinkConnectorConfig(map);

        LOGGER.info("Started Restful sink task");
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        LOGGER.debug("Received {} records", collection.size());

        List<SinkRecord> records = new ArrayList<>(collection);
        for (SinkRecord record : records) {
            Map<String, Object> jsonMap = (HashMap) record.value();
            final Gson gson = new Gson();
            final String json = gson.toJson(jsonMap);
            try {
                Class<? extends URIProvider> messageClass = (Class<? extends URIProvider>) Class.forName(restfulSinkConnectorConfig.getMessageClass());
                Class<?> entityClass = Class.forName(restfulSinkConnectorConfig.getEntityClass());
                Class<?> entitySerializerClass = Class.forName(restfulSinkConnectorConfig.getEntitySerializerClass());

                final GsonBuilder gsonBuilder = new GsonBuilder();

                Constructor<?> serializerCons = entitySerializerClass.getConstructor();

                gsonBuilder.registerTypeAdapter(entityClass, serializerCons.newInstance());
                gsonBuilder.setPrettyPrinting();
                final Gson entityGson = gsonBuilder.create();

                URIProvider messageObject = entityGson.fromJson(json, messageClass);

                Constructor<?> cons = entityClass.getConstructor(messageClass);

                Object entityObject = cons.newInstance(messageObject);
                String entityJson = entityGson.toJson(entityObject);

                URI uri;
                try {
                    uri = new URIBuilder().setScheme("http").setHost(messageObject.getHost())
                            .setPort(messageObject.getPort()).setPath(messageObject.getPath())
                            .build();
                } catch (URISyntaxException e) {
                    throw new ConnectException("The URL supplied is invalid!");
                }
                HttpPost httpPost = new HttpPost(uri);

                StringEntity requestEntity = new StringEntity(entityJson, ContentType.APPLICATION_JSON);
                httpPost.setEntity(requestEntity);

                LOGGER.info("Performing request: POST " + uri.toString()
                        + " with body: " + entityJson);
                try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                    HttpResponse response = httpClient.execute(httpPost);
                    LOGGER.info("Received response with status " + response.getStatusLine().getStatusCode() +
                            " and body: " + EntityUtils.toString(response.getEntity()));
                } catch (Exception e) {
                    throw new RetriableException("There was a problem processign records. Check that the provided URL is valid. Will retry.", e);
                }
            } catch (NoSuchMethodException | ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        LOGGER.debug("Finished processing the records");
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // Not used
    }

    @Override
    public void stop() {
        // Not used
    }
}
