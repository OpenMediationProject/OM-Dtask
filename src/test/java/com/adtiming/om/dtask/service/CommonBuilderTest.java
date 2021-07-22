package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.Application;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ActiveProfiles("dev")
public class CommonBuilderTest {

    @Autowired
    private CommonCacheBuilder commonCacheBuilder;

    @Resource
    private GoogleUarService googleUarService;

    @Test
    public void buildCountry() {
        commonCacheBuilder.buildCountry();
    }

    @Test
    public void buildCurrency() {
        commonCacheBuilder.buildCurrency();
    }

    @Test
    public void buildDict() {
        commonCacheBuilder.buildDict();
    }

    @Test
    public void topUarCalculate() {
        googleUarService.topUarCalculate();
    }
}
