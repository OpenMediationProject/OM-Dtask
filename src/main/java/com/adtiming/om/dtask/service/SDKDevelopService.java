// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * Created by huangqiang on 2019/1/24.
 * SDKDevelopService
 */
@Service
public class SDKDevelopService {
    private static final Logger log = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Scheduled(fixedDelay = 60000)
    private void updateSdkDevAppStatus() {
        if (!cfg.isProd())
            return;
        try {
            String sql = "select id from om_dev_app where status=1 and date_sub(active_time, interval -1 hour)<now()";
            List<Long> ids = jdbcTemplate.queryForList(sql, Long.class);
            if (!ids.isEmpty()) {
                String update = String.format("update om_dev_app set status=0 where id in (%s)", StringUtils.join(ids, ','));
                jdbcTemplate.update(update);
            }
        } catch (Exception e) {
            log.error("updateSdkDevAppStatus error", e);
        }
    }
}
