// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.web;

import com.adtiming.om.dtask.aws.DcenterJob;
import com.adtiming.om.dtask.dto.MailSender;
import com.adtiming.om.dtask.dto.ReportBuilderDTO;
import com.adtiming.om.dtask.service.ReportBuilderService;
import com.adtiming.om.dtask.util.Constants;
import com.adtiming.om.dtask.util.Util;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@RestController
public class APIController {

    private static final Logger log = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private DcenterJob dcenterJob;

    @Resource
    private ReportBuilderService reportBuilderService;

    /**
     * used for om-server init
     *
     * @return snode config
     */
    @GetMapping("/snode/config/get")
    public ResponseEntity<?> getSnodeConfig(
            HttpServletRequest req,
            @RequestParam("nodeid") String nodeid,
            @RequestParam("dcenter") int dcenter) {
        log.debug("snode/config/get, nodeid: {}, dcenter: {}", nodeid, dcenter);
        String sql = "select id,name,kafka_status,kafka_servers," +
                "s3_status,s3_region,s3_bucket,s3_access_key_id,s3_secret_access_key" +
                " from om_server_dcenter where id=?";
        JSONObject o = new JSONObject(jdbcTemplate.queryForMap(sql, dcenter));
        o.put("dcenter", dcenter);
        if (o.getIntValue("kafka_status") == 0) {
            o.remove("kafka_servers");
        }
        if (o.getIntValue("s3_status") == 0) {
            o.remove("s3_region");
            o.remove("s3_bucket");
            o.remove("s3_access_key_id");
            o.remove("s3_secret_access_key");
        }

        sql = "select id from om_server_node where nodeid=?";
        List<Integer> existsId = jdbcTemplate.queryForList(sql, Integer.class, nodeid);
        if (existsId.isEmpty()) {
            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(conn -> {
                String insertSql = "insert into om_server_node (nodeid, dcenter, ip) values (?,?,?)";
                PreparedStatement ps = conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
                ps.setString(1, nodeid);
                ps.setInt(2, dcenter);
                ps.setString(3, Util.getClientIP(req));
                return ps;
            }, keyHolder);
            o.put("id", keyHolder.getKey().intValue());
        } else {
            o.put("id", existsId.get(0));
        }

        return ResponseEntity.ok(o);
    }

    /**
     * back fill common report data, hourly within give time (beginDateHour <= the time  < endDateHour)
     *
     * @param beginDateHour begin date and hour, format: yyyyMMddHH
     * @param endDateHour   end date and hour , format: yyyyMMddHH
     */
    @RequestMapping("/dc/backfill/common/report")
    public ResponseEntity<?> backfillCommonReport(String beginDateHour, String endDateHour) {
        new Thread(new Runnable() {
            LocalDateTime beginDateTime = LocalDateTime.parse(beginDateHour, Constants.FORMATTER_YYYYMMDDHH);
            final LocalDateTime endDateTime = LocalDateTime.parse(endDateHour, Constants.FORMATTER_YYYYMMDDHH);

            @Override
            public void run() {
                try {
                    log.info("backfill common report, beginDateHour: {}, endDateHour: {}, start", beginDateHour, endDateHour);
                    while (beginDateTime.isBefore(endDateTime)) {
                        log.info("backfill common report, deal: {}", beginDateTime);
                        dcenterJob.commonReport(beginDateTime);
                        beginDateTime = beginDateTime.plusHours(1);
                    }
                    log.info("backfill common report, beginDateHour: {}, endDateHour: {}, complete", beginDateHour, endDateHour);
                } catch (Exception e) {
                    log.error("backfill common report error", e);
                }
            }
        }, "backfillCommonReport").start();
        return ResponseEntity.ok("success");
    }

    /**
     * back fill user report data, hourly within give date (beginDate <= the time  < endDate)
     *
     * @param beginDate begin date, format: yyyyMMdd
     * @param endDate   end date, format: yyyyMMdd
     */
    @RequestMapping("/dc/backfill/user/report")
    public ResponseEntity<?> backfillUserReport(String beginDate, String endDate) {
        new Thread(new Runnable() {
            LocalDate beginLocalDate = LocalDate.parse(beginDate, Constants.FORMATTER_YYYYMMDD);
            final LocalDate endLoaclDate = LocalDate.parse(endDate, Constants.FORMATTER_YYYYMMDD);

            @Override
            public void run() {
                try {
                    log.info("backfill user report, beginDate: {}, endDate: {}, start", beginDate, endDate);
                    while (beginLocalDate.isBefore(endLoaclDate)) {
                        log.info("backfill user report, deal: {}", beginLocalDate);
                        dcenterJob.userReport(beginLocalDate);
                        beginLocalDate = beginLocalDate.plusDays(1);
                    }
                    log.info("backfill user report, beginDate: {}, endDate: {}, complete", beginDate, endDate);
                } catch (Exception e) {
                    log.error("backfill user report error", e);
                }
            }
        }, "backfillUserReport").start();
        return ResponseEntity.ok("success");
    }

    /**
     * back fill user ad revenue data, hourly within give date (beginDate <= the time  < endDate)
     *
     * @param beginDate begin date, format: yyyyMMdd
     * @param endDate   end date, format: yyyyMMdd
     */
    @RequestMapping("/dc/backfill/user/ad/revenue")
    public ResponseEntity<?> backfillUserAdRevenue(String beginDate, String endDate) {
        new Thread(new Runnable() {
            LocalDate beginLocalDate = LocalDate.parse(beginDate, Constants.FORMATTER_YYYYMMDD);
            final LocalDate endLoaclDate = LocalDate.parse(endDate, Constants.FORMATTER_YYYYMMDD);

            @Override
            public void run() {
                try {
                    log.info("backfill user ad revenue, beginDate: {}, endDate: {}, start", beginDate, endDate);
                    while (beginLocalDate.isBefore(endLoaclDate)) {
                        log.info("backfill user ad revenue, deal: {}", beginLocalDate);
                        dcenterJob.userAdRevenue(beginLocalDate);
                        beginLocalDate = beginLocalDate.plusDays(1);
                    }
                    log.info("backfill user ad revenue, beginDate: {}, endDate: {}, complete", beginDate, endDate);
                } catch (Exception e) {
                    log.error("backfill user ad revenue error", e);
                }
            }
        }, "backfillUserAdRevenue").start();
        return ResponseEntity.ok("success");
    }

    @GetMapping("/api/customreport/test")
    public Object customReportTest(long id) {
        try {
            ReportBuilderDTO config = reportBuilderService.getReportBuilder(id);
            if (config == null) {
                return ResponseEntity.badRequest().body("config not exists");
            }
            if (StringUtils.isBlank(config.getRecipients())) {
                return ResponseEntity.badRequest().body("Empty Recipients");
            }
            MailSender mailSender = reportBuilderService.getMailSender();
            if (mailSender == null) {
                return ResponseEntity.accepted().body("mailSender not found");
            }
            config.setTaskDay(LocalDate.now().plusDays(-1));
            List<String> reportLines = reportBuilderService.buildReport(config);
            String result = StringUtils.join(reportLines, '\n');
            boolean sendStatus = reportBuilderService.sendToUser(config, result, mailSender);
            if (sendStatus) {
                return ResponseEntity.ok("OK");
            }
            return ResponseEntity.accepted().body("Send Failed");
        } catch (Exception e) {
            log.error("customReportTest error", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Test Failed");
        }
    }


}
