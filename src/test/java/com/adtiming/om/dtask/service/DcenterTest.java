package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.Application;
import com.adtiming.om.dtask.aws.DcenterJob;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.LocalDateTime;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ActiveProfiles("dev")
public class DcenterTest {

    @Resource
    private DcenterJob dcenterJob;

    @Test
    public void testHourly() {
        LocalDateTime executeDateTime = LocalDateTime.of(2020, 11, 2, 11, 0);
        dcenterJob.collectDatas(executeDateTime);
        dcenterJob.commonReport(executeDateTime);
    }

    @Test
    public void testDaily() {
        LocalDate executeDate = LocalDate.of(2020,11,2);
        dcenterJob.userReport(executeDate);
        dcenterJob.collectDwsPublisherUser(executeDate);
        dcenterJob.syncOdsOmAdnetwork2Athena(executeDate);
        dcenterJob.syncOdsStatAdnetwork2Athena(executeDate);
        dcenterJob.userAdRevenue(executeDate);
        dcenterJob.ltvReport(executeDate);
    }


}
