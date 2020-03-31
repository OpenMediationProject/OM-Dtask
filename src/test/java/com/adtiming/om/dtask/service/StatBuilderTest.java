package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.Application;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ActiveProfiles("dev")
public class StatBuilderTest {

    @Autowired
    private StatBuilder statBuilder;

    @Test
    public void buildPlacementCountryEcpm() {
        statBuilder.buildPlacementCountryEcpm();
    }

    @Test
    public void buildInstanceCountryEcpm() {
        statBuilder.buildInstanceCountryEcpm();
    }

    @Test
    public void buildPlacementEcpm() {
        statBuilder.buildPlacementEcpm();
    }

    @Test
    public void buildCountryAdTypeEcpm() {
        statBuilder.buildCountryAdTypeEcpm();
    }

}
