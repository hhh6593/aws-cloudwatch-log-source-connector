package com.github.hans;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

@Slf4j
public class AWSCloudwatchLogSourceConfig extends AbstractConfig {
    public static final String AWS_REGION = "aws.region";
    private static final String AWS_REGION_DOC = "AWS Profile region";
    public static final String AWS_CLOUDWATCH_LOG_GROUP = "aws.cloudwatch.log.group";
    private static final String AWS_CLOUDWATCH_LOG_GROUP_DOC = "Cloudwatch Log group Name";
    public static final String AWS_CLOUDWATCH_LOG_STREAM = "aws.cloudwatch.log.stream";
    private static final String AWS_CLOUDWATCH_LOG_STREAM_DOC = "Cloudwatch Log stream Name in aws.cloudwatch.log.group";
    public static final String KAFKA_TOPIC = "topic.name";
    private static final String KAFKA_TOPIC_DOC = "The name of the topic where you want to store the record";



    public AWSCloudwatchLogSourceConfig(Map<?, ?> originals) {
        super(config(), originals);
        this.originals();
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(AWS_REGION, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, AWS_REGION_DOC)
                .define(AWS_CLOUDWATCH_LOG_GROUP, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, AWS_CLOUDWATCH_LOG_GROUP_DOC)
                .define(AWS_CLOUDWATCH_LOG_STREAM, ConfigDef.Type.STRING, "",ConfigDef.Importance.LOW, AWS_CLOUDWATCH_LOG_STREAM_DOC)
                .define(KAFKA_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, KAFKA_TOPIC_DOC);
    }

}