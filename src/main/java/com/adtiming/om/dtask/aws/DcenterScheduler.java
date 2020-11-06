// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.aws;

import com.adtiming.om.dtask.service.AppConfig;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;

import javax.annotation.Resource;

@Component
public class DcenterScheduler {

    @Resource
    private AppConfig cfg;

    @Resource
    private AwsConfig awsConfig;

    @Resource
    private DcenterJob dcenterJob;

    @Scheduled(cron = "0 17 * * * ?")
    public void hourly() {
        if (!cfg.isProd()) {
            return;
        }
        if (awsConfig.isDisabled()) {
            return;
        }
        LocalDateTime executeDateTime = LocalDateTime.now().plusHours(-1);
        dcenterJob.collectDatas(executeDateTime);
        dcenterJob.commonReport(executeDateTime);
    }

    @Scheduled(cron = "0 5 1 * * ?")
    public void daily() {
        if (!cfg.isProd()) {
            return;
        }
        if (awsConfig.isDisabled()) {
            return;
        }
        LocalDate executeDate = LocalDate.now().plusDays(-1);
        dcenterJob.userReport(executeDate);
        dcenterJob.collectDwsPublisherUser(executeDate);
        dcenterJob.syncOdsOmAdnetwork2Athena(executeDate);
        for (int i = 0; i < 3; i++) {
            LocalDate tmpDate = executeDate.plusDays(-i);
            dcenterJob.syncOdsStatAdnetwork2Athena(tmpDate);
            dcenterJob.userAdRevenue(tmpDate);
            dcenterJob.ltvReport(tmpDate);
        }
        dcenterJob.clearTmpLocalDataDirectory(executeDate.plusDays(-7));
    }
}
