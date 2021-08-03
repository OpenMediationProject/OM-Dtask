// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud.huawei;

import com.adtiming.om.dtask.cloud.CloudService;
import com.adtiming.om.dtask.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Lazy;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class HuaweiCloudService implements CloudService {
    private static final Logger LOG = LogManager.getLogger();

    @Resource
    @Lazy
    private HuaweiCloudJob huaweiCloudJob;

    @Override
    public void backfillCollectData(String tableName, String beginDateHour, String endDateHour) {
        new Thread(new Runnable() {
            LocalDateTime beginDateTime = LocalDateTime.parse(beginDateHour, Constants.FORMATTER_YYYYMMDDHH);
            final LocalDateTime endDateTime = LocalDateTime.parse(endDateHour, Constants.FORMATTER_YYYYMMDDHH);

            @Override
            public void run() {
                try {
                    LOG.info("backfill collect data, table: {}, beginDateHour: {}, endDateHour: {}, start", tableName, beginDateHour, endDateHour);
                    while (beginDateTime.isBefore(endDateTime)) {
                        LOG.info("backfill collect data, table: {}, deal: {}", tableName, beginDateTime);
                        huaweiCloudJob.collectDatas(tableName, beginDateTime);
                        beginDateTime = beginDateTime.plusHours(1);
                    }
                    LOG.info("backfill collect data, table: {} beginDateHour: {}, endDateHour: {}, complete", tableName, beginDateHour, endDateHour);
                } catch (Exception e) {
                    LOG.error("backfill collect data report error", e);
                }
            }
        }, "backfillCollectData").start();
    }

    @Override
    public void backfillCommonReport(String beginDateHour, String endDateHour) {
        new Thread(new Runnable() {
            LocalDateTime beginDateTime = LocalDateTime.parse(beginDateHour, Constants.FORMATTER_YYYYMMDDHH);
            final LocalDateTime endDateTime = LocalDateTime.parse(endDateHour, Constants.FORMATTER_YYYYMMDDHH);

            @Override
            public void run() {
                try {
                    LOG.info("backfill common report, beginDateHour: {}, endDateHour: {}, start", beginDateHour, endDateHour);
                    while (beginDateTime.isBefore(endDateTime)) {
                        LOG.info("backfill common report, deal: {}", beginDateTime);
                        huaweiCloudJob.commonReport(beginDateTime);
                        beginDateTime = beginDateTime.plusHours(1);
                    }
                    LOG.info("backfill common report, beginDateHour: {}, endDateHour: {}, complete", beginDateHour, endDateHour);
                } catch (Exception e) {
                    LOG.error("backfill common report error", e);
                }
            }
        }, "backfillCommonReport").start();
    }

    @Override
    public void backfillCpReport(String beginDateHour, String endDateHour) {
        new Thread(new Runnable() {
            LocalDateTime beginDateTime = LocalDateTime.parse(beginDateHour, Constants.FORMATTER_YYYYMMDDHH);
            final LocalDateTime endDateTime = LocalDateTime.parse(endDateHour, Constants.FORMATTER_YYYYMMDDHH);

            @Override
            public void run() {
                try {
                    LOG.info("backfill cp report, beginDateHour: {}, endDateHour: {}, start", beginDateHour, endDateHour);
                    while (beginDateTime.isBefore(endDateTime)) {
                        LOG.info("backfill cp report, deal: {}", beginDateTime);
                        huaweiCloudJob.cpReport(beginDateTime);
                        beginDateTime = beginDateTime.plusHours(1);
                    }
                    LOG.info("backfill cp report, beginDateHour: {}, endDateHour: {}, complete", beginDateHour, endDateHour);
                } catch (Exception e) {
                    LOG.error("backfill cp report error", e);
                }
            }
        }, "backfillCpReport").start();
    }

    @Override
    public void backfillUserReport(String beginDate, String endDate) {
        new Thread(new Runnable() {
            LocalDate beginLocalDate = LocalDate.parse(beginDate, Constants.FORMATTER_YYYYMMDD);
            final LocalDate endLoaclDate = LocalDate.parse(endDate, Constants.FORMATTER_YYYYMMDD);

            @Override
            public void run() {
                try {
                    LOG.info("backfill user report, beginDate: {}, endDate: {}, start", beginDate, endDate);
                    while (beginLocalDate.isBefore(endLoaclDate)) {
                        LOG.info("backfill user report, deal: {}", beginLocalDate);
                        huaweiCloudJob.userReport(beginLocalDate);
                        beginLocalDate = beginLocalDate.plusDays(1);
                    }
                    LOG.info("backfill user report, beginDate: {}, endDate: {}, complete", beginDate, endDate);
                } catch (Exception e) {
                    LOG.error("backfill user report error", e);
                }
            }
        }, "backfillUserReport").start();
    }

    @Override
    public void backfillUserAdRevenue(String beginDate, String endDate) {
        new Thread(new Runnable() {
            LocalDate beginLocalDate = LocalDate.parse(beginDate, Constants.FORMATTER_YYYYMMDD);
            final LocalDate endLoaclDate = LocalDate.parse(endDate, Constants.FORMATTER_YYYYMMDD);

            @Override
            public void run() {
                try {
                    LOG.info("backfill user ad revenue, beginDate: {}, endDate: {}, start", beginDate, endDate);
                    while (beginLocalDate.isBefore(endLoaclDate)) {
                        LOG.info("backfill user ad revenue, deal: {}", beginLocalDate);
                        huaweiCloudJob.userAdRevenue(beginLocalDate);
                        beginLocalDate = beginLocalDate.plusDays(1);
                    }
                    LOG.info("backfill user ad revenue, beginDate: {}, endDate: {}, complete", beginDate, endDate);
                } catch (Exception e) {
                    LOG.error("backfill user ad revenue error", e);
                }
            }
        }, "backfillUserAdRevenue").start();
    }

    @Override
    public void backfillPublisherUser(String beginDate, String endDate) {
        new Thread(new Runnable() {
            LocalDate beginLocalDate = LocalDate.parse(beginDate, Constants.FORMATTER_YYYYMMDD);
            final LocalDate endLoaclDate = LocalDate.parse(endDate, Constants.FORMATTER_YYYYMMDD);

            @Override
            public void run() {
                try {
                    LOG.info("backfill publisher user, beginDate: {}, endDate: {}, start", beginDate, endDate);
                    while (beginLocalDate.isBefore(endLoaclDate)) {
                        LOG.info("backfill publisher user, deal: {}", beginLocalDate);
                        huaweiCloudJob.collectDwsPublisherUser(beginLocalDate);
                        beginLocalDate = beginLocalDate.plusDays(1);
                    }
                    LOG.info("backfill publisher user, beginDate: {}, endDate: {}, complete", beginDate, endDate);
                } catch (Exception e) {
                    LOG.error("backfill publisher user error", e);
                }
            }
        }, "backfillPublisherUser").start();
    }

    @Override
    public void backfillLtvReport(String beginDate, String endDate) {
        new Thread(new Runnable() {
            LocalDate beginLocalDate = LocalDate.parse(beginDate, Constants.FORMATTER_YYYYMMDD);
            final LocalDate endLoaclDate = LocalDate.parse(endDate, Constants.FORMATTER_YYYYMMDD);

            @Override
            public void run() {
                try {
                    LOG.info("backfill ltv report, beginDate: {}, endDate: {}, start", beginDate, endDate);
                    while (beginLocalDate.isBefore(endLoaclDate)) {
                        LOG.info("backfill ltv report, deal: {}", beginLocalDate);
                        huaweiCloudJob.ltvReport(beginLocalDate);
                        beginLocalDate = beginLocalDate.plusDays(1);
                    }
                    LOG.info("backfill ltv report, beginDate: {}, endDate: {}, complete", beginDate, endDate);
                } catch (Exception e) {
                    LOG.error("backfill ltv report error", e);
                }
            }
        }, "backfillLtvReport").start();
    }
}
