// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Service
public class AppConfig {

    private static final Logger log = LogManager.getLogger();

    public final ZoneOffset TZ = ZoneOffset.UTC;
    public final DateTimeFormatter LOG_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    public enum AppEnv {
        prod, dev
    }

    @Value("${app.env}")
    public AppEnv appEnv;

    public final File dir = new File("cache");

    private boolean shouldStop = false;

    @Value("${currency.api.appkey}")
    public String currencyApiKey;

    @PostConstruct
    private void init() {
        if (!dir.exists() && dir.mkdir())
            log.info("mkdir {}", dir);
    }

    @PreDestroy
    private void destroy() {
        shouldStop = true;
    }

    public boolean isProd() {
        return appEnv == AppEnv.prod;
    }

    public boolean isStopping() {
        return shouldStop;
    }

}
