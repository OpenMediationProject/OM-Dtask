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
public class SDKCacheBuilderTest {

    @Autowired
    private SDKCacheBuilder builder;

    @Test
    public void buildPublisherApp() {
        builder.buildPublisherApp();
    }

    @Test
    public void buildPlacement() {
        builder.buildPlacement();
    }

    @Test
    public void buildAdNetowrk() {
        builder.buildAdNetowrk();
    }

    @Test
    public void buildAdNetowrkApp() {
        builder.buildAdNetowrkApp();
    }

    @Test
    public void buildInstance() {
        builder.buildInstance();
    }

    @Test
    public void buildInstanceRule() {
        builder.buildInstanceRule();
    }

    @Test
    public void buildSdkDevApp() {
        builder.buildSdkDevApp();
    }

    @Test
    public void buildSdkDevDevice() {
        builder.buildSdkDevDevice();
    }

    @Test
    public void buildAbTest() {
        builder.buildAbTest();
    }

}
