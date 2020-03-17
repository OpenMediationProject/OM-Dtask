// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

import javax.annotation.PostConstruct;

public class AwsConfig {

    private static final Logger LOG = LogManager.getLogger();

    public static final RowMapper<AwsConfig> ROWMAPPER = BeanPropertyRowMapper.newInstance(AwsConfig.class);

    private int s3Status;
    private String s3Region;
    private String s3Bucket;
    private String s3AccessKeyId;
    private String s3SecretAccessKey;

    private AWSCredentialsProvider credentialsProvider;
    private AmazonS3 amazonS3;
    private AmazonAthena amazonAthena;

    @PostConstruct
    private void init() {
        if (isDisabled()) {
            LOG.info("aws s3 is disabled");
            return;
        }

        if (StringUtils.isEmpty(s3AccessKeyId)) {
            credentialsProvider = new EC2ContainerCredentialsProviderWrapper();
        } else {
            credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3AccessKeyId, s3SecretAccessKey));
        }

        amazonS3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.fromName(s3Region))
                .withCredentials(credentialsProvider)
                .build();
        LOG.info("init AmazonS3");

        amazonAthena = AmazonAthenaClientBuilder.standard()
                .withRegion(s3Region)
                .withClientConfiguration(new ClientConfiguration()
                        .withClientExecutionTimeout(AthenaConstants.CLIENT_EXECUTION_TIMEOUT))
                .withCredentials(credentialsProvider)
                .build();
        LOG.info("init AmazonAthena");
    }

    public boolean isDisabled() {
        return s3Status != 1;
    }

    public AmazonAthena getAmazonAthena() {
        return amazonAthena;
    }

    public AmazonS3 getAmazonS3() {
        return amazonS3;
    }

    public int getS3Status() {
        return s3Status;
    }

    public void setS3Status(int s3Status) {
        this.s3Status = s3Status;
    }

    public String getS3Region() {
        return s3Region;
    }

    public void setS3Region(String s3Region) {
        this.s3Region = s3Region;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public String getS3AccessKeyId() {
        return s3AccessKeyId;
    }

    public void setS3AccessKeyId(String s3AccessKeyId) {
        this.s3AccessKeyId = s3AccessKeyId;
    }

    public String getS3SecretAccessKey() {
        return s3SecretAccessKey;
    }

    public void setS3SecretAccessKey(String s3SecretAccessKey) {
        this.s3SecretAccessKey = s3SecretAccessKey;
    }
}
