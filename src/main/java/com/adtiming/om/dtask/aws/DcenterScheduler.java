// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.aws;

import com.adtiming.om.dtask.service.AppConfig;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Component
public class DcenterScheduler {

    @Resource
    private AppConfig cfg;

    @Resource
    private AwsConfig awsConfig;

    @Resource
    private DcenterJob dcenterJob;

    @Scheduled(cron = "0 17 * * * ?", zone = "UTC")
    public void hourly() {
        if (!cfg.isProd()) {
            return;
        }
        if (awsConfig.isDisabled()) {
            return;
        }
        LocalDateTime executeDateTime = LocalDateTime.now(ZoneOffset.UTC).plusHours(-1);
        dcenterJob.addPartition(executeDateTime);
        dcenterJob.commonReport(executeDateTime);
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
        executeDate = executeDate.plusDays(-7);
        dcenterJob.clearDataDirectory(executeDate);
    }
}
