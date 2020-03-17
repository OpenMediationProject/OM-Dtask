// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import com.adtiming.om.pb.CommonPB;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class CommonCacheBuilder extends PbBuiler {

    private static final Logger log = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    private Lock lock = new ReentrantLock();

    @Scheduled(fixedDelay = 2 * 60000)
    public void buildCache() {
        if (!cfg.isProd())
            return;
        try {
            lock.lock();
            log.debug("start to build cache");
            long start = System.currentTimeMillis();
            buildDict();
            buildCountry();
            buildCurrency();
            log.debug("build cache finished, cost {} ms", System.currentTimeMillis() - start);
        } finally {
            lock.unlock();
        }
    }

    void buildCountry() {
        build("om_country", cfg.dir, out -> {
            String sql = "SELECT a2,a3,tz,dcenter FROM om_country";
            jdbcTemplate.query(sql, rs -> {
                out.writeDelimited(CommonPB.Country.newBuilder()
                        .setA2(rs.getString("a2"))
                        .setA3(rs.getString("a3"))
                        .setTz(rs.getInt("tz"))
                        .setDcenter(rs.getInt("dcenter"))
                        .build());
            });
        });

    }

    void buildDict() {
        build("om_dict", cfg.dir, out -> {
            String sql = "SELECT id,pid,name,value FROM om_dict";
            jdbcTemplate.query(sql, rs -> {
                out.writeDelimited(CommonPB.Dict.newBuilder()
                        .setId(rs.getInt("id"))
                        .setPid(rs.getInt("pid"))
                        .setName(rs.getString("name"))
                        .setValue(StringUtils.defaultIfEmpty(rs.getString("value"), ""))
                        .build());
            });
        });
    }

    void buildCurrency() {
        build("om_currency", cfg.dir, out -> {
            String sql = "SELECT cur_from,cur_to,exchange_rate FROM om_currency_exchange";
            jdbcTemplate.query(sql, rs -> {
                out.writeDelimited(CommonPB.Currency.newBuilder()
                        .setCurFrom(rs.getString("cur_from"))
                        .setCurTo(rs.getString("cur_to"))
                        .setExchangeRate(rs.getFloat("exchange_rate"))
                        .build());
            });
        });
    }

}
