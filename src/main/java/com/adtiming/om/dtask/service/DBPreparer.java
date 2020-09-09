// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Service
public class DBPreparer {

    private static final Logger log = LogManager.getLogger();

    private static final DateTimeFormatter FMT_DAY = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter FMT_MONTH = DateTimeFormatter.ofPattern("yyyyMM");

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Scheduled(cron = "0 0 1 * * ?", zone = "UTC")
    private void createDayPartitions() {
        createDayPartitions("stat_adnetwork");
        createDayPartitions("stat_lr");
        createDayPartitions("stat_dau");
    }

    private void createDayPartitions(String table) {
        StringBuilder buf = new StringBuilder(100);
        buf.append("alter table ").append(table).append(" add partition(");
        LocalDate day = LocalDate.now(ZoneOffset.UTC).plusDays(1);
        String dayStr = FMT_DAY.format(day);
        String endDayStr = FMT_DAY.format(day.plusDays(1));
        buf.append(" partition p").append(dayStr).append(" values less than (to_days(").append(endDayStr).append("))");
        buf.append(" )");

        String sql = buf.toString();
        try {
            jdbcTemplate.update(sql);
        } catch (DataAccessException e) {
            log.error("create partition error, sql: {}", sql, e);
        }
    }

    @Scheduled(cron = "0 0 1 1 * ?", zone = "UTC")
    private void createMonthPartitions() {
        createMonthPartitions("report_adnetwork_linked");
        createMonthPartitions("report_adnetwork_task");
        createMonthPartitions("report_adtiming");
        createMonthPartitions("report_admob");
        createMonthPartitions("report_facebook");
        createMonthPartitions("report_unity");
        createMonthPartitions("report_vungle");
        createMonthPartitions("report_adcolony");
        createMonthPartitions("report_applovin");
        createMonthPartitions("report_mopub");
        createMonthPartitions("report_tapjoy");
        createMonthPartitions("report_chartboost");
        createMonthPartitions("report_tiktok");
        createMonthPartitions("report_mintegral");
        createMonthPartitions("report_ironsource");
        createMonthPartitions("report_mint");
    }

    private void createMonthPartitions(String table) {
        StringBuilder buf = new StringBuilder(100);
        buf.append("alter table ").append(table).append(" add partition(");
        LocalDate day = LocalDate.now(ZoneOffset.UTC).plusMonths(1).withDayOfMonth(1);
        String monthStr = FMT_MONTH.format(day);
        String endDayStr = FMT_DAY.format(day.plusMonths(1));
        buf.append(" partition p").append(monthStr).append(" values less than (to_days(").append(endDayStr).append("))");
        buf.append(" )");

        String sql = buf.toString();
        try {
            jdbcTemplate.update(sql);
        } catch (DataAccessException e) {
            log.error("create month partition error, sql: {}", sql, e);
        }
    }

}
