// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud.huawei;

import com.adtiming.om.dtask.cloud.CloudConstants;
import com.huawei.dli.sdk.Queue;
import com.huawei.dli.sdk.SQLJob;
import com.huawei.dli.sdk.common.JobStatus;
import com.huawei.dli.sdk.exception.DLIException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;

public class HuaweiDliExecutor {
    private static final Logger LOG = LogManager.getLogger();

    private final Queue dliQueue;

    public HuaweiDliExecutor(HuaweiCloudClient client) {
        this.dliQueue = client.getDliQueue();
    }

    public String submitDliQuery(String databases, String sql) {
        SQLJob sqlJob = new SQLJob(dliQueue, databases, sql);
        sqlJob.setConf(Collections.singletonList(Collections.singletonMap("spark.sql.session.timeZone", "UTC")));
        try {
            sqlJob.submit();
        } catch (DLIException e) {
            LOG.error("query failed, job submit error.", e);
            throw new RuntimeException("query failed, job submit error", e);
        }
        String jobId = sqlJob.getJobId();
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            JobStatus jobStatus;
            try {
                jobStatus = sqlJob.getStatus();
            } catch (DLIException e) {
                LOG.error("query failed, job get status error.", e);
                throw new RuntimeException("query failed, job get status error", e);
            }
            switch (jobStatus) {
                case FINISHED:
                    isQueryStillRunning = false;
                    break;
                case FAILED:
                    LOG.error("query failed, job id: {}", jobId);
                    throw new RuntimeException("query failed, job id: " + jobId);
                case CANCELLED:
                    LOG.error("query was cancelled, job id: {}", jobId);
                    throw new RuntimeException("query was cancelled, job id: " + jobId);
                default:
                    try {
                        Thread.sleep(CloudConstants.SLEEP_AMOUNT_IN_MS);
                    } catch (InterruptedException e) {
                        LOG.error("query was sleep, job id: {}", jobId, e);
                    }
            }
        }
        return jobId;
    }
}
