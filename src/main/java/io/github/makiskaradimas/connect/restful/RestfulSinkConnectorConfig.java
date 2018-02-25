package io.github.makiskaradimas.connect.restful;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestfulSinkConnectorConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(RestfulSinkConnectorConfig.class);

	public static final String MESSAGE_CLASS = "message.class";
	public static final String ENTITY_CLASS = "entity.class";
	public static final String ENTITY_SERIALIZER_CLASS = "entity.serializer.class";
	public static final String BULK_SIZE = "bulk.size";

    private String messageClass;
    private String entityClass;
    private String entitySerializerClass;
	private String bulkSize;

	public static final ConfigDef CONFIG_DEF = new ConfigDef().define(BULK_SIZE, Type.STRING, Importance.HIGH, "Not of items to send");

	public static RestfulSinkConnectorConfig getRestFulSinkConnectorConfig(Map<String, String> map) {
		LOGGER.trace("Reading the configuration");

		RestfulSinkConnectorConfig restfulSinkConnectorConfig = new RestfulSinkConnectorConfig();

        restfulSinkConnectorConfig.setMessageClass(map.get(MESSAGE_CLASS));
        restfulSinkConnectorConfig.setEntityClass(map.get(ENTITY_CLASS));
        restfulSinkConnectorConfig.setEntitySerializerClass(map.get(ENTITY_SERIALIZER_CLASS));
		restfulSinkConnectorConfig.setBulkSize(map.get(BULK_SIZE));
		restfulSinkConnectorConfig.validateConfig();

		LOGGER.trace("Finished reading the configuration");

		return restfulSinkConnectorConfig;
	}

	public void validateConfig() {
		LOGGER.trace("Validating the configuration");

		if (messageClass == null || messageClass.isEmpty())
			throw new ConnectException("Missing " + MESSAGE_CLASS + " config");

        if (entityClass == null || entityClass.isEmpty())
            throw new ConnectException("Missing " + ENTITY_CLASS + " config");

        if (entitySerializerClass == null || entitySerializerClass.isEmpty())
            throw new ConnectException("Missing " + ENTITY_SERIALIZER_CLASS + " config");

		if (bulkSize == null || bulkSize.isEmpty())
			throw new ConnectException("Missing " + BULK_SIZE + " config");

		LOGGER.trace("Finished validating the configuration");
	}

	public Map<String, String> getMap() {
		Map<String, String> config = new HashMap<>();

        config.put(MESSAGE_CLASS, messageClass);
        config.put(ENTITY_CLASS, entityClass);
        config.put(ENTITY_SERIALIZER_CLASS, entitySerializerClass);
		config.put(BULK_SIZE, bulkSize);

		return config;
	}

    public String getMessageClass() {
        return messageClass;
    }

    public void setMessageClass(String messageClass) {
        this.messageClass = messageClass;
    }

    public String getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(String entityClass) {
        this.entityClass = entityClass;
    }

    public String getEntitySerializerClass() {
        return entitySerializerClass;
    }

    public void setEntitySerializerClass(String entitySerializerClass) {
        this.entitySerializerClass = entitySerializerClass;
    }

    public String getBulkSize() {
		return bulkSize;
	}

	public void setBulkSize(String bulkSize) {
		this.bulkSize = bulkSize;
	}

}
