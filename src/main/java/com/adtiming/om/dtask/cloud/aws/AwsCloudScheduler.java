// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud.aws;

import com.adtiming.om.dtask.service.AppConfig;
import com.adtiming.om.dtask.service.DictManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class AwsCloudScheduler {

    @Value("${aws.athena.data-dir}")
    private String awsAtenaDataDir;

    @Resource
    private AppConfig cfg;

    @Resource
    private AwsCloudJob dcenterJob;

    @Resource
    private DictManager dictManager;

    @Scheduled(cron = "0 17 * * * ?", zone = "UTC")
    public void hourly() {
        if (cfg.isDev()) {
            return;
        }
        LocalDateTime executeDateTime = LocalDateTime.now(ZoneOffset.UTC).plusHours(-1);
        dcenterJob.collectDatas(executeDateTime);
        dcenterJob.commonReport(executeDateTime);
        dcenterJob.cpReport(executeDateTime);
    }

    @Scheduled(cron = "0 5 1 * * ?", zone = "UTC")
    public void daily() {
        if (cfg.isDev()) {
            return;
        }
        LocalDate executeDate = LocalDate.now(ZoneOffset.UTC).plusDays(-1);
        dcenterJob.userReport(executeDate);
        dcenterJob.collectDwsPublisherUser(executeDate);
        dcenterJob.syncOdsOmAdnetwork2Athena(executeDate);
        dcenterJob.pubAppCountryReport(executeDate);
        for (int i = 0; i < 3; i++) {
            LocalDate tmpDate = executeDate.plusDays(-i);
            dcenterJob.syncOdsStatAdnetwork2Athena(tmpDate);

            if (dcenterJob.isSwitchOn(dictManager.intVal("/om/uar_switch"))) {
                dcenterJob.userAdRevenue(tmpDate);
            }

            if (dcenterJob.isSwitchOn(dictManager.intVal("/om/ltv_switch"))) {
                dcenterJob.ltvReport(tmpDate);
            }
        }
        dcenterJob.clearTmpLocalDataDirectory(awsAtenaDataDir, executeDate.plusDays(-7));
    }

}
