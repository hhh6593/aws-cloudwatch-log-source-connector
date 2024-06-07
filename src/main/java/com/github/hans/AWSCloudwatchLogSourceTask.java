package com.github.hans;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class AWSCloudwatchLogSourceTask extends SourceTask {
    private CloudWatchLogsClient cloudWatchLogsClient;
    private AWSCloudwatchLogSourceConfig config;
    private String logGroup;
    private String topicName;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting cloudwatch log source task");
        try {
            config = new AWSCloudwatchLogSourceConfig(props);
            logGroup = config.getString(AWSCloudwatchLogSourceConfig.AWS_CLOUDWATCH_LOG_GROUP);
            topicName = logGroup.replace("/","-").substring(1);
            initializeCloudWatchLogsClient();

        } catch (ConfigException e) {
            throw new ConfigException("Couldn't start AWSCloudwatchLogSourceTask due to configuration error", e);
        }
    }

    /**
     * AWS CloudWatchLogsClient 초기화
     */
    private void initializeCloudWatchLogsClient() {
        try {
            Region awsRegion = Region.of(config.getString(AWSCloudwatchLogSourceConfig.AWS_REGION));

            cloudWatchLogsClient = CloudWatchLogsClient.builder()
                    .region(awsRegion)
                    .build();
        } catch (Exception e) {
            log.error("Error initializing CloudWatchLogsClient: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = new ArrayList<>();
        FilterLogEventsRequest logEventsRequest = FilterLogEventsRequest.builder()
                .logGroupName(logGroup)
                .build();

        // 오프셋 정보가 있는지 확인
        Map<String,Object> offset = context.offsetStorageReader().offset(Collections.singletonMap("logGroup", logGroup));
        if (offset != null) {
            Object lastRecordedOffset = offset.get("timestamp");
            if (lastRecordedOffset!= null && !(lastRecordedOffset instanceof Long)) {
                throw new ConfigException("Offset must be of type Long");
            }
            if (lastRecordedOffset != null) {
                log.debug("Resuming from offset: {}", lastRecordedOffset);
                logEventsRequest = logEventsRequest.toBuilder().startTime((Long) lastRecordedOffset).build();
            }
        } else {
            log.info("No offset found, starting from the current time {}", logGroup);
            logEventsRequest = logEventsRequest.toBuilder().startTime(System.currentTimeMillis() - 1000 * 60 * 60 * 24).build();
        }

        FilterLogEventsResponse logEventsResponse = cloudWatchLogsClient.filterLogEvents(logEventsRequest);
        Map<String, String> sourcePartition = Collections.singletonMap("logGroup", logGroup);

        for (FilteredLogEvent event : logEventsResponse.events()) {
            String eventMessage = event.message();
            Map<String, Long> sourceOffset = Map.of("timestamp", event.timestamp());
            SourceRecord record = new SourceRecord(
                    sourcePartition,
                    sourceOffset,
                    topicName,
                    Schema.STRING_SCHEMA,
                    eventMessage
            );
            records.add(record);
        }
        return records;
    }

    @Override
    public void stop() {
        log.info("Stopping AWS Cloudwatch log source task");

        if (cloudWatchLogsClient != null) {
            cloudWatchLogsClient.close();
        }
    }
}
