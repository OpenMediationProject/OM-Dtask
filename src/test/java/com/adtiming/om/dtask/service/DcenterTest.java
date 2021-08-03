package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.Application;
import com.adtiming.om.dtask.cloud.CloudJob;
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
    private CloudJob dcenterJob;

    @Test
    public void testHourly() {
        LocalDateTime executeDateTime = LocalDateTime.of(2020, 12, 1, 8, 0);
        dcenterJob.collectDatas(executeDateTime);
        dcenterJob.commonReport(executeDateTime);
        dcenterJob.cpReport(executeDateTime);
    }

    @Test
    public void testDaily() {
        LocalDate executeDate = LocalDate.of(2021, 7, 30);
        dcenterJob.userReport(executeDate);
        dcenterJob.collectDwsPublisherUser(executeDate);
//        dcenterJob.syncOdsOmAdnetwork2Athena(executeDate);
//        dcenterJob.syncOdsStatAdnetwork2Athena(executeDate);
        dcenterJob.userAdRevenue(executeDate);
        dcenterJob.ltvReport(executeDate);
        dcenterJob.pubAppCountryReport(executeDate);
    }


}
