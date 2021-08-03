// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.web;

import com.adtiming.om.dtask.cloud.CloudService;
import com.adtiming.om.dtask.dto.MailSender;
import com.adtiming.om.dtask.dto.ReportBuilderDTO;
import com.adtiming.om.dtask.service.ReportBuilderService;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.util.List;

@RestController
public class JobController {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    @Lazy
    private CloudService cloudService;

    @Resource
    private ReportBuilderService reportBuilderService;

    /**
     * backfill collect data, hourly within give time (beginDateHour <= the time  < endDateHour)
     *
     * @param tableName     tablename
     * @param beginDateHour begin date and hour, format: yyyyMMddHH
     * @param endDateHour   end date and hour , format: yyyyMMddHH
     */
    @RequestMapping("/dc/backfill/collect/data")
    public ResponseEntity<?> backfillCollectData(String tableName, String beginDateHour, String endDateHour) {
        cloudService.backfillCollectData(tableName, beginDateHour, endDateHour);
        return ResponseEntity.ok("success");
    }

    /**
     * backfill common report data, hourly within give time (beginDateHour <= the time  < endDateHour)
     *
     * @param beginDateHour begin date and hour, format: yyyyMMddHH
     * @param endDateHour   end date and hour , format: yyyyMMddHH
     */
    @RequestMapping("/dc/backfill/common/report")
    public ResponseEntity<?> backfillCommonReport(String beginDateHour, String endDateHour) {
        cloudService.backfillCommonReport(beginDateHour, endDateHour);
        return ResponseEntity.ok("success");
    }

    /**
     * backfill cp report data, hourly within give time (beginDateHour <= the time  < endDateHour)
     *
     * @param beginDateHour begin date and hour, format: yyyyMMddHH
     * @param endDateHour   end date and hour , format: yyyyMMddHH
     */
    @RequestMapping("/dc/backfill/cp/report")
    public ResponseEntity<?> backfillCpReport(String beginDateHour, String endDateHour) {
        cloudService.backfillCpReport(beginDateHour, endDateHour);
        return ResponseEntity.ok("success");
    }

    /**
     * backfill user report data, hourly within give date (beginDate <= the time  < endDate)
     *
     * @param beginDate begin date, format: yyyyMMdd
     * @param endDate   end date, format: yyyyMMdd
     */
    @RequestMapping("/dc/backfill/user/report")
    public ResponseEntity<?> backfillUserReport(String beginDate, String endDate) {
        cloudService.backfillUserReport(beginDate, endDate);
        return ResponseEntity.ok("success");
    }

    /**
     * backfill user ad revenue data, hourly within give date (beginDate <= the time  < endDate)
     *
     * @param beginDate begin date, format: yyyyMMdd
     * @param endDate   end date, format: yyyyMMdd
     */
    @RequestMapping("/dc/backfill/user/ad/revenue")
    public ResponseEntity<?> backfillUserAdRevenue(String beginDate, String endDate) {
        cloudService.backfillUserAdRevenue(beginDate, endDate);
        return ResponseEntity.ok("success");
    }

    /**
     * backfill publisher user data, hourly within give date (beginDate <= the time  < endDate)
     * plealse
     *
     * @param beginDate begin date, format: yyyyMMdd
     * @param endDate   end date, format: yyyyMMdd
     */
    @RequestMapping("/dc/backfill/publisher/user")
    public ResponseEntity<?> backfillPublisherUser(String beginDate, String endDate) {
        cloudService.backfillPublisherUser(beginDate, endDate);
        return ResponseEntity.ok("success,  Note: Please delete all the partitions from the beginDate !!! ");
    }

    /**
     * backfill ltv report, hourly within give date (beginDate <= the time  < endDate)
     *
     * @param beginDate begin date, format: yyyyMMdd
     * @param endDate   end date, format: yyyyMMdd
     */
    @RequestMapping("/dc/backfill/ltv/report")
    public ResponseEntity<?> backfillLtvReport(String beginDate, String endDate) {
        cloudService.backfillLtvReport(beginDate, endDate);
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
            LOG.error("customReportTest error", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Test Failed");
        }
    }
}
