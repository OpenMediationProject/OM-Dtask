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
public class StatInstanceBuilderTest {

    @Autowired
    private StatInstanceBuilder builder;

    @Test
    public void buildInstanceCountryEcpm() {
        builder.buildInstanceCountryEcpm("stat_instance_country_ecpm_3h", 3);
        builder.buildInstanceCountryEcpm("stat_instance_country_ecpm_6h", 6);
        builder.buildInstanceCountryEcpm("stat_instance_country_ecpm_12h", 12);
        builder.buildInstanceCountryEcpm("stat_instance_country_ecpm_24h", 24);
    }

    @Test
    public void buildAdNetworkCountryEcpm() {
        builder.buildAdNetworkCountryEcpm();
    }

    @Test
    public void buildAdNetworkAdTypeCountryEcpm3d() {
        builder.buildAdNetworkAdTypeCountryEcpm3d();
    }

    @Test
    public void buildAdNetworkEcpm3d() {
        builder.buildAdNetworkEcpm3d();
    }

}
