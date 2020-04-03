// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.web;

import com.adtiming.om.dtask.aws.DcenterJob;
import com.adtiming.om.dtask.util.Constants;
import com.alibaba.fastjson.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.LocalDateTime;

@RestController
public class APIController {

    private static final Logger log = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private DcenterJob dcenterJob;

    @GetMapping("/snode/config/get")
    public ResponseEntity<?> getSnodeConfig(
            @RequestParam("id") String id,
            @RequestParam("dcenter") int dcenter) {
        log.debug("snode/config/get, id: {}, dcenter: {}", id, dcenter);
        String sql = "select id,name,kafka_status,kafka_servers," +
                "s3_status,s3_region,s3_bucket,s3_access_key_id,s3_secret_access_key" +
                " from om_server_dcenter where id=?";
        JSONObject o = new JSONObject(jdbcTemplate.queryForMap(sql, dcenter));
        if (o.getIntValue("kafka_status") == 0) {
            o.remove("kafka_servers");
        }
        if (o.getIntValue("s3_status") == 0) {
            o.remove("s3_region");
            o.remove("s3_bucket");
            o.remove("s3_access_key_id");
            o.remove("s3_secret_access_key");
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
            LocalDateTime endDateTime = LocalDateTime.parse(endDateHour, Constants.FORMATTER_YYYYMMDDHH);

            @Override
            public void run() {
                try {
                    log.info("backfill common report, beginDateHour: {}, endDateHour: {}, start", beginDateHour, endDateHour);
                    while (beginDateTime.isBefore(endDateTime)) {
                        log.info("backfill common report, deal: {}", beginDateTime);
                        dcenterJob.addPartition(beginDateTime);
                        dcenterJob.commonReport(beginDateTime);
                        beginDateTime = beginDateTime.plusHours(1);
                    }
                    log.info("backfill common report, beginDateHour: {}, endDateHour: {}, complete", beginDateHour, endDateHour);
                } catch (Exception e) {
                    log.error("backfill common report error", e);
                }
            }
        }).start();
        return ResponseEntity.ok("success");
    }

    /**
     * back fill user report data, hourly within give date (beginDate <= the time  < endDate)
     *
     * @param beginDate begin date, format: yyyyMMddHH
     * @param endDate   end date, format: yyyyMMddHH
     */
    @RequestMapping("/dc/backfill/user/report")
    public ResponseEntity<?> backfillUserReport(String beginDate, String endDate) {
        new Thread(new Runnable() {
            LocalDate beginLocalDate = LocalDate.parse(beginDate, Constants.FORMATTER_YYYYMMDD);
            LocalDate endLoaclDate = LocalDate.parse(endDate, Constants.FORMATTER_YYYYMMDD);

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
        }).start();
        return ResponseEntity.ok("success");
    }


}
