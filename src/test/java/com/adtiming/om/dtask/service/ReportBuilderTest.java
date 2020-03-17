package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.Application;
import com.adtiming.om.dtask.dto.DataSourceType;
import com.adtiming.om.dtask.dto.ReportBuilderDTO;
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
//        reportBuilderService.buildTasks();
//        reportBuilderService.doTasks();
        reportBuilderService.scheduleReport();
    }

    @Test
    public void testSendToUser() {
        ReportBuilderDTO dto = new ReportBuilderDTO();
        dto.setTaskDay(LocalDate.now());
        dto.setName("testReport");
        dto.setRecipients("jseparator@qq.com");
        reportBuilderService.sendToUser(dto, "test,test\n1,1");
    }

    @Test
    public void testGetAdNetworkReport() {
        List<ReportBuilderDTO> builderDTOS = reportBuilderService.getReportBuilders();
        for (ReportBuilderDTO builderDTO : builderDTOS) {
            this.reportBuilderService.getAdNetWorkReport(builderDTO);
        }
    }

    @Test
    public void testGetLrReport() {
        List<ReportBuilderDTO> builderDTOS = reportBuilderService.getReportBuilders();
        for (ReportBuilderDTO builderDTO : builderDTOS) {
            this.reportBuilderService.getLRReport(builderDTO);
        }
    }

    @Test
    public void testGetPerformanceReport() {
        List<ReportBuilderDTO> builderDTOS = reportBuilderService.getReportBuilders();
        for (ReportBuilderDTO builderDTO : builderDTOS) {
            builderDTO.setDataSource(DataSourceType.PERFORMANCE.ordinal());
            builderDTO.setTaskDay(LocalDate.now().plusDays(-1));
            this.reportBuilderService.buildReport(builderDTO);
        }
    }

    @Test
    public void testGetUserActivityReport() {
        List<ReportBuilderDTO> builderDTOS = reportBuilderService.getReportBuilders();
        for (ReportBuilderDTO builderDTO : builderDTOS) {
            builderDTO.setDataSource(DataSourceType.USER_ACTIVITY.ordinal());
            builderDTO.setTaskDay(LocalDate.now().plusDays(-1));
            this.reportBuilderService.buildReport(builderDTO);
        }
    }
}
