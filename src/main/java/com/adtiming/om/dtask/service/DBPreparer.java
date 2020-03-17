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

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Scheduled(cron = "0 0 1 * * ?", zone = "UTC")
    private void createPartitions() {
        create("stat_adnetwork");
        create("stat_lr");
        create("stat_dau");
    }

    private void create(String table) {
        StringBuilder buf = new StringBuilder(100);
        buf.append("alter table ").append(table).append(" add partition(");
        LocalDate day = LocalDate.now(ZoneOffset.UTC).plusDays(1);
        String dayStr = FMT.format(day);
        String endDayStr = FMT.format(day.plusDays(1));
        buf.append(" partition p").append(dayStr).append(" values less than (to_days(").append(endDayStr).append("))");
        buf.append(" )");

        String sql = buf.toString();
        try {
            jdbcTemplate.update(sql);
        } catch (DataAccessException e) {
            log.error("create partition error, sql: {}", sql, e);
        }
    }

}
