// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud.aws;

import com.adtiming.om.dtask.cloud.CloudConstants;
import com.adtiming.om.dtask.cloud.CloudClient;
import com.alibaba.fastjson.JSON;
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
import org.springframework.web.context.support.GenericWebApplicationContext;

public class AwsCloudClient implements CloudClient {

    private static final Logger LOG = LogManager.getLogger();

    private final AmazonS3 amazonS3;
    private final AmazonAthena amazonAthena;
    private final Config cfg;

    private static class Config {
        public String ak, sk, region, bucket;
    }

    public AwsCloudClient(String cloudConfigJson) {
        cfg = JSON.parseObject(cloudConfigJson, Config.class);
        AWSCredentialsProvider credentialsProvider;
        if (StringUtils.isEmpty(cfg.ak)) {
            credentialsProvider = new EC2ContainerCredentialsProviderWrapper();
        } else {
            credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(cfg.ak, cfg.sk));
        }

        amazonS3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.fromName(cfg.region))
                .withCredentials(credentialsProvider)
                .build();
        LOG.info("init AmazonS3");

        amazonAthena = AmazonAthenaClientBuilder.standard()
                .withRegion(cfg.region)
                .withClientConfiguration(new ClientConfiguration()
                        .withClientExecutionTimeout(CloudConstants.CLIENT_EXECUTION_TIMEOUT))
                .withCredentials(credentialsProvider)
                .build();
        LOG.info("init AmazonAthena");
    }

    @Override
    public void registerBeans(GenericWebApplicationContext context) {
        context.registerBean(AwsAthenaExecutor.class, () -> new AwsAthenaExecutor(this));
        context.registerBean(AwsS3Executor.class, () -> new AwsS3Executor(this));
        context.registerBean(AwsCloudJob.class, () -> new AwsCloudJob(this));
        context.registerBean(AwsCloudService.class, AwsCloudService::new);
        context.registerBean(AwsCloudScheduler.class, AwsCloudScheduler::new);
        context.getBean(AwsCloudScheduler.class); // get bean for init
    }

    public String getBucket() {
        return cfg.bucket;
    }

    public AmazonAthena getAmazonAthena() {
        return amazonAthena;
    }

    public AmazonS3 getAmazonS3() {
        return amazonS3;
    }

}
