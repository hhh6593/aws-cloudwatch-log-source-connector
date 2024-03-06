package com.github.hans;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

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
    private String topic;
    private boolean latestStream = false;
    private long nextStartTime;
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
            topic = config.getString(AWSCloudwatchLogSourceConfig.KAFKA_TOPIC);
            initializeCloudWatchLogsClient();
            setupLogGroupAndStream();
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

    private void setupLogGroupAndStream() {
        try {
            logGroup = config.getString(AWSCloudwatchLogSourceConfig.AWS_CLOUDWATCH_LOG_GROUP);
            logStream = config.getString(AWSCloudwatchLogSourceConfig.AWS_CLOUDWATCH_LOG_STREAM);

            if (logGroup.isEmpty()) {
                throw new ConfigException("aws.cloudwatch.log.group is required.");
            }

            if (logStream.isEmpty()) {
                logStream = getLatestLogStream();
                latestStream = true;
            }
        } catch (Exception e) {
            log.error("Error setting up log group and stream: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public List<SourceRecord> poll() {
        if (latestStream && nextStartTime > lastEventTimestamp) {
            logStream = getLatestLogStream();
        }

        GetLogEventsRequest logEventsRequest = GetLogEventsRequest.builder()
                .logGroupName(logGroup)
                .logStreamName(logStream)
                .startTime(nextStartTime)
                .build();

        GetLogEventsResponse logEventsResponse = cloudWatchLogsClient.getLogEvents(logEventsRequest);

        List<SourceRecord> records = new ArrayList<>();
        Map<String, String> sourcePartition = Collections.singletonMap("logStream", logStream);
        for (OutputLogEvent event : logEventsResponse.events()) {
            String eventMessage = event.message();
            Map<String, Long> sourceOffset = Map.of("timestamp", event.timestamp());
            SourceRecord record = new SourceRecord(
                    sourcePartition,
                    sourceOffset,
                    topic,
                    Schema.STRING_SCHEMA,
                    eventMessage
            );
            records.add(record);
            nextStartTime = event.timestamp() + 1;
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
