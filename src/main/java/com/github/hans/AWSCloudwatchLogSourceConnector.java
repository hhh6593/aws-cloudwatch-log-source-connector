package com.github.hans;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class AWSCloudwatchLogSourceConnector extends SourceConnector {

    private Map<String, String> configProps;
    @Override
    public void start(Map<String, String> props) {
        log.info("Starting AWS Cloudwatch log Source Connector {}", props);
        configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AWSCloudwatchLogSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();

        configs.add(configProps);
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping AWS Cloudwatch Log Source Connector");
    }

    @Override
    public ConfigDef config() {
        return AWSCloudwatchLogSourceConfig.config();
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

}
