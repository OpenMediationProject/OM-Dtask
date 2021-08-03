// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud.huawei;

import com.adtiming.om.dtask.cloud.CloudClient;
import com.alibaba.fastjson.JSON;
import com.huawei.dli.sdk.DLIClient;
import com.huawei.dli.sdk.Queue;
import com.huawei.dli.sdk.authentication.AuthenticationMode;
import com.huawei.dli.sdk.common.DLIInfo;
import com.huawei.dli.sdk.exception.DLIException;
import com.obs.services.ObsClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.context.support.GenericWebApplicationContext;

public class HuaweiCloudClient implements CloudClient {

    private static final Logger LOG = LogManager.getLogger();

    private final Config cfg;
    private final RdsConfig rdsConfig;
    private final ObsClient obsClient;
    private final DLIClient dliClient;
    private final Queue dliQueue;

    private static class Config {
        public String ak, sk, region, endpoint, bucket, projectId, queue, rdsJdbcUrl, rdsDatabase, rdsPasswdAuth;
    }

    public static class RdsConfig {
        private final String rdsJdbcUrl, rdsDatabase, rdsPasswdAuth;

        public RdsConfig(String rdsJdbcUrl, String rdsDatabase, String rdsPasswdAuth) {
            this.rdsJdbcUrl = rdsJdbcUrl;
            this.rdsDatabase = rdsDatabase;
            this.rdsPasswdAuth = rdsPasswdAuth;
        }

        public String getRdsJdbcUrl() {
            return rdsJdbcUrl;
        }

        public String getRdsDatabase() {
            return rdsDatabase;
        }

        public String getRdsPasswdAuth() {
            return rdsPasswdAuth;
        }
    }

    public HuaweiCloudClient(String cloudConfigJson) throws DLIException {
        cfg = JSON.parseObject(cloudConfigJson, Config.class);
        rdsConfig = new RdsConfig(cfg.rdsJdbcUrl, cfg.rdsDatabase, cfg.rdsPasswdAuth);
        obsClient = new ObsClient(cfg.ak, cfg.sk, cfg.endpoint);
        DLIInfo dliInfo = new DLIInfo(cfg.region, cfg.ak, cfg.sk, cfg.projectId);
        dliClient = new DLIClient(AuthenticationMode.AKSK, dliInfo);
        dliQueue = dliClient.getQueue(cfg.queue);
        LOG.info("init huawei cloud client complete");
    }

    @Override
    public void registerBeans(GenericWebApplicationContext context) {
        context.registerBean(HuaweiObsExecutor.class, () -> new HuaweiObsExecutor(this));
        context.registerBean(HuaweiDliExecutor.class, () -> new HuaweiDliExecutor(this));
        context.registerBean(HuaweiCloudJob.class, () -> new HuaweiCloudJob(this));
        context.registerBean(HuaweiCloudService.class, HuaweiCloudService::new);
        context.registerBean(HuaweiCloudScheduler.class, HuaweiCloudScheduler::new);
        context.getBean(HuaweiCloudScheduler.class); // get bean for init
    }

    public String getBucket() {
        return cfg.bucket;
    }

    public ObsClient getObsClient() {
        return obsClient;
    }

    public DLIClient getDliClient() {
        return dliClient;
    }

    public Queue getDliQueue() {
        return dliQueue;
    }

    public RdsConfig getRdsConfig() {
        return rdsConfig;
    }
}
