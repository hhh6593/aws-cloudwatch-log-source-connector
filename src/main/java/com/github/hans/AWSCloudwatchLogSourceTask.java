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
        Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap("logGroup", logGroup));

        Long lastProcessedTimestamp;
        String lastProcessedEventId;

        if (offset != null) {
            lastProcessedTimestamp = (Long) offset.get("timestamp");
            lastProcessedEventId = (String) offset.get("eventId");
        } else {
            log.info("No offset found, starting from 24 hours ago for logGroup {}", logGroup);
            lastProcessedTimestamp = System.currentTimeMillis() - 1000 * 60 * 60 * 24;
            lastProcessedEventId = null;
        }

        Long startTime = lastProcessedTimestamp;
        String nextToken = null;

        Long maxTimestamp = lastProcessedTimestamp;
        String maxEventId = lastProcessedEventId;

        do {
            FilterLogEventsRequest.Builder requestBuilder = FilterLogEventsRequest.builder()
                    .logGroupName(logGroup)
                    .startTime(startTime);

            if (nextToken != null) {
                requestBuilder.nextToken(nextToken);
            }

            FilterLogEventsResponse logEventsResponse = cloudWatchLogsClient.filterLogEvents(requestBuilder.build());

            for (FilteredLogEvent event : logEventsResponse.events()) {
                Long eventTimestamp = event.timestamp();
                String eventId = event.eventId();

                // 이미 처리된 이벤트를 스킵하여 중복 방지
                if (eventTimestamp < lastProcessedTimestamp) {
                    continue;
                } else if (eventTimestamp.equals(lastProcessedTimestamp)) {
                    if (lastProcessedEventId != null && eventId.compareTo(lastProcessedEventId) <= 0) {
                        continue;
                    }
                }

                String eventMessage = event.message();
                Map<String, Object> sourceOffset = Map.of(
                        "timestamp", eventTimestamp,
                        "eventId", eventId
                );

                SourceRecord record = new SourceRecord(
                        Collections.singletonMap("logGroup", logGroup),
                        sourceOffset,
                        topicName,
                        Schema.STRING_SCHEMA,
                        eventMessage
                );
                records.add(record);

                // 가장 큰 타임스탬프와 이벤트 ID를 업데이트
                if (eventTimestamp > maxTimestamp || (eventTimestamp.equals(maxTimestamp) && eventId.compareTo(maxEventId) > 0)) {
                    maxTimestamp = eventTimestamp;
                    maxEventId = eventId;
                }
            }

            nextToken = logEventsResponse.nextToken();
        } while (nextToken != null && !nextToken.isEmpty());

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
