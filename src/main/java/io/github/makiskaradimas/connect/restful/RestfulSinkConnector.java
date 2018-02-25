package io.github.makiskaradimas.connect.restful;

import io.github.makiskaradimas.connect.utils.LogUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RestfulSinkConnector extends SinkConnector {
	private final static Logger log = LoggerFactory.getLogger(RestfulSinkConnector.class);

	private RestfulSinkConnectorConfig restfulSinkConnectorConfig;
	
	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> map) {
		restfulSinkConnectorConfig = RestfulSinkConnectorConfig.getRestFulSinkConnectorConfig(map);
		LogUtils.dumpConfiguration(map, log);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return RestfulSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>();

		for (int i = 0; i < maxTasks; i++) {
			configs.add(restfulSinkConnectorConfig.getMap());
		}
		return configs;
	}

	@Override
	public void stop() {

	}

	@Override
	public ConfigDef config() {
		return RestfulSinkConnectorConfig.CONFIG_DEF;
	}
}
