// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud.aws;

import com.adtiming.om.dtask.cloud.CloudConstants;
import com.amazonaws.services.athena.model.GetQueryExecutionRequest;
import com.amazonaws.services.athena.model.GetQueryExecutionResult;
import com.amazonaws.services.athena.model.QueryExecutionContext;
import com.amazonaws.services.athena.model.QueryExecutionState;
import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;

public class AwsAthenaExecutor {

    private static final Logger LOG = LogManager.getLogger();

    @Value("${aws.athena.workgroup}")
    private String athenaWorkGroup;

    private final AwsCloudClient client;

    public AwsAthenaExecutor(AwsCloudClient client) {
        this.client = client;
    }

    public String submitAthenaQuery(String databases, String sql, String outPutDirectory) {
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest()
                .withQueryString(sql)
                .withQueryExecutionContext(new QueryExecutionContext().withDatabase(databases))
                .withResultConfiguration(new ResultConfiguration().withOutputLocation(outPutDirectory));
        if (StringUtils.isNotEmpty(athenaWorkGroup)) {
            startQueryExecutionRequest.withWorkGroup(athenaWorkGroup);
        }
        return client.getAmazonAthena().startQueryExecution(startQueryExecutionRequest).getQueryExecutionId();
    }

    public void waitForQueryToComplete(String queryExecutionId) {
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            GetQueryExecutionResult getQueryExecutionResult = client.getAmazonAthena()
                    .getQueryExecution(new GetQueryExecutionRequest().withQueryExecutionId(queryExecutionId));
            QueryExecutionState queryExecutionState = QueryExecutionState.fromValue(getQueryExecutionResult.getQueryExecution().getStatus().getState());
            switch (queryExecutionState) {
                case FAILED:
                    String stateChangeReason = getQueryExecutionResult.getQueryExecution().getStatus().getStateChangeReason();
                    LOG.error("query failed, id: {}, msg: {}", queryExecutionId, stateChangeReason);
                    throw new RuntimeException("query failed, id: " + queryExecutionId + ", msg: " + stateChangeReason);
                case CANCELLED:
                    LOG.error("query was cancelled, id: {}", queryExecutionId);
                    throw new RuntimeException("query was cancelled, id: " + queryExecutionId);
                case SUCCEEDED:
                    isQueryStillRunning = false;
                    break;
                default:
                    try {
                        Thread.sleep(CloudConstants.SLEEP_AMOUNT_IN_MS);
                    } catch (InterruptedException e) {
                        LOG.error("query was sleep, id: {}", queryExecutionId, e);
                    }
            }
        }
    }
}
