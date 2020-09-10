package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.Application;
import com.adtiming.om.dtask.aws.DcenterJob;
import com.adtiming.om.dtask.aws.DcenterScheduler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ActiveProfiles("dev")
public class DcenterTest {

    @Resource
    private DcenterScheduler dcenterScheduler;

    @Resource
    private DcenterJob dcenterJob;


    @Test
    public void testHourly() {
        dcenterScheduler.hourly();
    }

    @Test
    public void testDaily() {
        dcenterScheduler.daily();
//        LocalDate executeDate = LocalDate.now(ZoneOffset.UTC).plusDays(-1);
//        executeDate = executeDate.withYear(2020).withMonth(6).withDayOfMonth(7);
//        dcenterJob.userAdRevenue(executeDate);
    }


}
