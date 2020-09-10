// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.aws;

import com.amazonaws.services.athena.model.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class AthenaExecutor {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AwsConfig awsConfig;

    public String submitAthenaQuery(String databases, String sql, String outPutDirectory) {
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest()
                .withQueryString(sql)
                .withQueryExecutionContext(new QueryExecutionContext().withDatabase(databases))
                .withResultConfiguration(new ResultConfiguration().withOutputLocation(outPutDirectory));
        return awsConfig.getAmazonAthena().startQueryExecution(startQueryExecutionRequest).getQueryExecutionId();
    }

    public void waitForQueryToComplete(String queryExecutionId) {
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            GetQueryExecutionResult getQueryExecutionResult = awsConfig.getAmazonAthena()
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
                        Thread.sleep(AthenaConstants.SLEEP_AMOUNT_IN_MS);
                    } catch (InterruptedException e) {
                        LOG.error("query was sleep, id: {}", queryExecutionId, e);
                    }
            }
        }
    }
}
