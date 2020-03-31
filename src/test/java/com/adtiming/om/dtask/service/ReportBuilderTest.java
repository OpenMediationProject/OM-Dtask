package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.Application;
import com.adtiming.om.dtask.dto.DataSourceType;
import com.adtiming.om.dtask.dto.MailSender;
import com.adtiming.om.dtask.dto.ReportBuilderDTO;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDate;
import java.util.List;

/**
 * Created by ruandianbo on 20-3-2.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ActiveProfiles("dev")
public class ReportBuilderTest {

    @Autowired
    private ReportBuilderService reportBuilderService;

    @Test
    public void testDoTask() {
        MailSender mailSender = reportBuilderService.getMailSender();
        List<ReportBuilderDTO> builderDTOS = reportBuilderService.getReportBuilders();
        for (ReportBuilderDTO builder : builderDTOS) {
            builder.setTaskDay(LocalDate.now().plusDays(-1));
            List<String> reportLines = reportBuilderService.buildReport(builder);
            String result = StringUtils.join(reportLines, '\n');
            boolean sendStatus = reportBuilderService.sendToUser(builder, result, mailSender);
            Assert.assertTrue(sendStatus);
            break;
        }
    }

    @Test
    public void testSendToUser() {
        MailSender mailSender = reportBuilderService.getMailSender();
        ReportBuilderDTO dto = new ReportBuilderDTO();
        dto.setTaskDay(LocalDate.now());
        dto.setName("testReport");
        dto.setRecipients("jseparator@qq.com");
        reportBuilderService.sendToUser(dto, "test,test\n1,1", mailSender);
    }

    @Test
    public void testGetAdNetworkReport() {
        List<ReportBuilderDTO> builderDTOS = reportBuilderService.getReportBuilders();
        for (ReportBuilderDTO builder : builderDTOS) {
            builder.setTaskDay(LocalDate.now().plusDays(-1));
            this.reportBuilderService.getAdNetWorkReport(builder);
        }
    }

    @Test
    public void testGetLrReport() {
        List<ReportBuilderDTO> builderDTOS = reportBuilderService.getReportBuilders();
        for (ReportBuilderDTO builder : builderDTOS) {
            builder.setTaskDay(LocalDate.now().plusDays(-1));
            this.reportBuilderService.getLRReport(builder);
        }
    }

    @Test
    public void testGetPerformanceReport() {
        List<ReportBuilderDTO> builderDTOS = reportBuilderService.getReportBuilders();
        for (ReportBuilderDTO builder : builderDTOS) {
            builder.setDataSource(DataSourceType.PERFORMANCE.ordinal());
            builder.setTaskDay(LocalDate.now().plusDays(-1));
            this.reportBuilderService.buildReport(builder);
        }
    }

    @Test
    public void testGetUserActivityReport() {
        List<ReportBuilderDTO> builderDTOS = reportBuilderService.getReportBuilders();
        for (ReportBuilderDTO builder : builderDTOS) {
            builder.setDataSource(DataSourceType.USER_ACTIVITY.ordinal());
            builder.setTaskDay(LocalDate.now().plusDays(-1));
            this.reportBuilderService.buildReport(builder);
        }
    }
}
