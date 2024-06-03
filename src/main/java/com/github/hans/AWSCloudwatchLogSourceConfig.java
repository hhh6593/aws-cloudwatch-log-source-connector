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
    public static final String START_FROM_LATEST = "start.from.latest";
    private static final String START_FROM_LATEST_DOC = "Start from latest log stream";


    public AWSCloudwatchLogSourceConfig(Map<?, ?> originals) {
        super(config(), originals);
        this.originals();
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(AWS_REGION, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, AWS_REGION_DOC)
                .define(AWS_CLOUDWATCH_LOG_GROUP, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, AWS_CLOUDWATCH_LOG_GROUP_DOC)
                .define(AWS_CLOUDWATCH_LOG_STREAM, ConfigDef.Type.STRING, "",ConfigDef.Importance.LOW, AWS_CLOUDWATCH_LOG_STREAM_DOC)
                .define(START_FROM_LATEST, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, START_FROM_LATEST_DOC);
    }

}