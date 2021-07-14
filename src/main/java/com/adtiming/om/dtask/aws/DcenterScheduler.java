// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.aws;

import com.adtiming.om.dtask.service.AppConfig;
import com.adtiming.om.dtask.service.DictManager;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Component
public class DcenterScheduler {
    private static final int SWITCH_ON = 1;

    @Resource
    private AppConfig cfg;

    @Resource
    private AwsConfig awsConfig;

    @Resource
    private DcenterJob dcenterJob;

    @Resource
    private DictManager dictManager;

    @Scheduled(cron = "0 17 * * * ?", zone = "UTC")
    public void hourly() {
        if (cfg.isDev()) {
            return;
        }
        if (awsConfig.isDisabled()) {
            return;
        }
        LocalDateTime executeDateTime = LocalDateTime.now(ZoneOffset.UTC).plusHours(-1);
        dcenterJob.collectDatas(executeDateTime);
        dcenterJob.commonReport(executeDateTime);
        dcenterJob.cpReport(executeDateTime);
    }

    @Scheduled(cron = "0 5 1 * * ?", zone = "UTC")
    public void daily() {
        if (!cfg.isProd()) {
            return;
        }
        if (awsConfig.isDisabled()) {
            return;
        }
        LocalDate executeDate = LocalDate.now(ZoneOffset.UTC).plusDays(-1);
        dcenterJob.userReport(executeDate);
        dcenterJob.collectDwsPublisherUser(executeDate);
        dcenterJob.syncOdsOmAdnetwork2Athena(executeDate);
        for (int i = 0; i < 3; i++) {
            LocalDate tmpDate = executeDate.plusDays(-i);
            dcenterJob.syncOdsStatAdnetwork2Athena(tmpDate);

            if (isSwitchOn(dictManager.intVal("/om/uar_switch"))) {
                dcenterJob.userAdRevenue(tmpDate);
            }

            if (isSwitchOn(dictManager.intVal("/om/ltv_switch"))) {
                dcenterJob.ltvReport(tmpDate);
            }
        }
        dcenterJob.clearTmpLocalDataDirectory(executeDate.plusDays(-7));
    }

    private static boolean isSwitchOn(int switchValue) {
        return SWITCH_ON == switchValue;
    }
}
