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
    private String logStream;
    private long lastEventTimestamp;

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
            initializeCloudWatchLogsClient();
            setupLogStream();
        } catch (ConfigException e) {
            throw new ConfigException("Couldn't start AWSCloudwatchLogSourceTask due to configuration error", e);
        }
    }

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

    /**
     * aws.cloudwatch.log.group에서 설정한 logStream을 지정하는 메서드.
     * start.from.latest 설정이 false일 경우 log stream을 필수로 설정해야 한다.
     */
    private void setupLogStream() {
        try {
            logStream = config.getString(AWSCloudwatchLogSourceConfig.AWS_CLOUDWATCH_LOG_STREAM);

            if (!config.getBoolean(AWSCloudwatchLogSourceConfig.START_FROM_LATEST)) {
                if (logStream.isEmpty()) {
                    throw new ConfigException("aws.cloudwatch.log.stream must be specified when start.from.latest is false");
                }
            } else {
                logStream = getLatestLogStream();
            }
        } catch (Exception e) {
            log.error("Error setting up log stream: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = new ArrayList<>();
        GetLogEventsRequest logEventsRequest = GetLogEventsRequest.builder()
                .logGroupName(logGroup)
                .logStreamName(logStream)
                .build();

        // 오프셋 정보가 있는지 확인
        Map<String,Object> offset = context.offsetStorageReader().offset(Collections.singletonMap("logStream", logStream));
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
            log.debug("No offset found, starting from the beginning at {}.{}", logGroup, logStream);
            logEventsRequest = logEventsRequest.toBuilder().startTime(0L).build();
        }

        GetLogEventsResponse logEventsResponse = cloudWatchLogsClient.getLogEvents(logEventsRequest);
        Map<String, String> sourcePartition = Collections.singletonMap("logStream", logStream);

        for (OutputLogEvent event : logEventsResponse.events()) {
            String eventMessage = event.message();
            Map<String, Long> sourceOffset = Map.of("timestamp", event.timestamp());
            SourceRecord record = new SourceRecord(
                    sourcePartition,
                    sourceOffset,
                    config.getString(AWSCloudwatchLogSourceConfig.AWS_CLOUDWATCH_LOG_GROUP).replace("/","-").substring(1),
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

    private String getLatestLogStream() {
        DescribeLogStreamsRequest request = DescribeLogStreamsRequest.builder()
                .logGroupName(logGroup)
                .orderBy("LastEventTime")
                .descending(true)
                .limit(1)
                .build();

        DescribeLogStreamsResponse response = cloudWatchLogsClient.describeLogStreams(request);

        if (!response.logStreams().isEmpty()) {
            LogStream latestLogStream = response.logStreams().get(0);
            lastEventTimestamp = latestLogStream.lastEventTimestamp();
            return latestLogStream.logStreamName();
        } else {
            log.error("No log streams found for the specified log group.");
            throw new RuntimeException("No log streams found.");
        }

    }
}
