// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Service
public class DBPreparer {

    private static final Logger log = LogManager.getLogger();

    private static final DateTimeFormatter FMT_DAY = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter FMT_MONTH = DateTimeFormatter.ofPattern("yyyyMM");

    private static final String[] DAY_PARTITION_TABLES = {
            "stat_adnetwork",
            "stat_lr",
            "stat_cp",
            "stat_dau",
            "stat_dau_adn",
            "stat_dau_placement",
            "stat_dau_adn_placement",
            "stat_dau_instance",
            "stat_user_ltv"
    };

    private static final String[] MONTH_PARTITION_TABLES = {
            "report_adnetwork_linked",
            "report_adnetwork_task",
            "report_adtiming",
            "report_admob",
            "report_facebook",
            "report_unity",
            "report_vungle",
            "report_adcolony",
            "report_applovin",
            "report_mopub",
            "report_tapjoy",
            "report_chartboost",
            "report_tiktok",
            "report_mintegral",
            "report_ironsource",
            "report_mint",
            "report_helium"
    };

    @Resource
    private JdbcTemplate jdbcTemplate;

    @PostConstruct
    private void init() {
        // make sure partitions exists
        createCurrentDayPartitions();
        createNextDayPartitions();

        createCurrentMonthPartitions();
        createNextMonthPartitions();
    }

    private void createCurrentDayPartitions() {
        for (String dayPartitionTable : DAY_PARTITION_TABLES) {
            createDayPartitions(dayPartitionTable, LocalDate.now(ZoneOffset.UTC));
        }
    }

    @Scheduled(cron = "0 0 1 * * ?", zone = "UTC")
    private void createNextDayPartitions() {
        for (String dayPartitionTable : DAY_PARTITION_TABLES) {
            createDayPartitions(dayPartitionTable, LocalDate.now(ZoneOffset.UTC).plusDays(1));
        }
    }

    private void createDayPartitions(String table, LocalDate day) {
        String partition = "p" + FMT_DAY.format(day);
        try {
            if (isPartitionExists(table, partition)) {
                log.info("partition {}.{} exist, ignore", table, partition);
                return;
            }
            String endDayStr = FMT_DAY.format(day.plusDays(1));
            String sql = "alter table " + table + " add partition(" +
                    " partition " + partition + " values less than (to_days(" + endDayStr + "))" +
                    " )";
            jdbcTemplate.update(sql);
        } catch (DataAccessException e) {
            log.error("create partition error", e);
        }
    }

    private void createCurrentMonthPartitions() {
        for (String monthPartitionTable : MONTH_PARTITION_TABLES) {
            createMonthPartitions(monthPartitionTable, LocalDate.now(ZoneOffset.UTC).withDayOfMonth(1));
        }
    }

    @Scheduled(cron = "0 0 1 1 * ?", zone = "UTC")
    private void createNextMonthPartitions() {
        for (String monthPartitionTable : MONTH_PARTITION_TABLES) {
            createMonthPartitions(monthPartitionTable, LocalDate.now(ZoneOffset.UTC).plusMonths(1).withDayOfMonth(1));
        }
    }

    private void createMonthPartitions(String table, LocalDate month) {
        String partition = "p" + FMT_MONTH.format(month);
        try {
            if (isPartitionExists(table, partition)) {
                log.info("partition {}.{} exist, ignore", table, partition);
                return;
            }
            String endDayStr = FMT_DAY.format(month.plusMonths(1));
            String sql = "alter table " + table + " add partition(" +
                    " partition " + partition + " values less than (to_days(" + endDayStr + "))" +
                    " )";
            jdbcTemplate.update(sql);
        } catch (DataAccessException e) {
            log.error("create month partition error", e);
        }
    }

    /**
     * check is table partition exists
     *
     * @param table     tableName
     * @param partition partitionName
     */
    private boolean isPartitionExists(String table, String partition) {
        String checkSql = "select count(*) from information_schema.PARTITIONS" +
                " where TABLE_SCHEMA=schema()" +
                " and TABLE_NAME=?" +
                " and PARTITION_NAME=?";
        return jdbcTemplate.queryForObject(checkSql, Integer.class, table, partition) > 0;
    }

}
