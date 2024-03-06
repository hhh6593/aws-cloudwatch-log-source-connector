package com.github.hans;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import software.amazon.awssdk.regions.Region;

import java.util.Map;

public class AWSCloudwatchSourceConfig extends AbstractConfig {
    private static final String AWS_CREDENTIAL_PROFILE = "aws.profile";
    private static final String AWS_CREDENTIAL_PROFILE_DOC = "AWS Credential Profile For AWS SDK";
    private static final String AWS_CLOUDWATCH_LOG_GROUP = "aws.cloudwatch.log.group";
    private static final String AWS_CLOUDWATCH_LOG_GROUP_DOC = "Cloudwatch Log Group Name";


    public AWSCloudwatchSourceConfig(Map<?, ?> originals) {
        super(config(), originals);
    }

    protected static ConfigDef config() {
        return new ConfigDef()
                .define(AWS_CREDENTIAL_PROFILE, ConfigDef.Type.STRING, "default", ConfigDef.Importance.MEDIUM, AWS_CREDENTIAL_PROFILE_DOC)
                .define(AWS_CLOUDWATCH_LOG_GROUP, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, AWS_CLOUDWATCH_LOG_GROUP_DOC);
    }

    public String getAwsCredentialProfile() {
        return this.getString(AWS_CREDENTIAL_PROFILE);
    }




}
