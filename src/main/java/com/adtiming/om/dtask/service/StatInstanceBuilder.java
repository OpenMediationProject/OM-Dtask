// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import com.adtiming.om.pb.StatPB;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.time.format.DateTimeFormatter.ISO_DATE;

/**
 * Created by huangqiang on 2019/11/12.
 * SDKInstanceStatBuilder
 */
@Service
public class StatInstanceBuilder extends PbBuiler {

    private static final Logger log = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;

    @Resource
    private JdbcTemplate jdbcTemplate;

    //AdNetwork api report delay in hours
    private Map<Integer, Integer> adNetworkDelay = new HashMap<>(20);

    @PostConstruct
    private void init() {
        adNetworkDelay.put(1, 1);//adtiming
        adNetworkDelay.put(2, 3);//admob
        adNetworkDelay.put(3, 9);//facebook
        adNetworkDelay.put(4, 2);//unity
        adNetworkDelay.put(5, 1);//vungle
        adNetworkDelay.put(6, 11);//tencentAd
        adNetworkDelay.put(7, 1);//adcolony
        adNetworkDelay.put(8, 1);//applovin
        adNetworkDelay.put(9, 47);//mopub
        adNetworkDelay.put(11, 1);//tapjoy
        adNetworkDelay.put(12, 9);//chartboost
        adNetworkDelay.put(13, 16);//TikTok
        adNetworkDelay.put(14, 2);//mintegral
        adNetworkDelay.put(15, 1);//ironSource
        adNetworkDelay.put(17, 12);//ChartboostBid
        adNetworkDelay.put(18, 10);//Mint
        adNetworkDelay.put(19, 1);//CrossPromotion
        adNetworkDelay.put(20, 14);//SigMob
        adNetworkDelay.put(21, 10);//KuaiShou
        adNetworkDelay.put(23, 10);//PubNative
        if (!cfg.isProd())
            return;
        threadPoolTaskScheduler.getScheduledExecutor().schedule(this::buildCache, 30, TimeUnit.SECONDS);
    }

    @Scheduled(cron = "0 0 * * * ?")
    private void buildCache() {
        if (!cfg.isProd())
            return;
        log.debug("build instance stat cache start");
        long start = System.currentTimeMillis();
        buildInstanceCountryEcpm("stat_instance_country_ecpm_3h", 3);
        buildInstanceCountryEcpm("stat_instance_country_ecpm_6h", 6);
        buildInstanceCountryEcpm("stat_instance_country_ecpm_12h", 12);
        buildInstanceCountryEcpm("stat_instance_country_ecpm_24h", 24);
        buildAdNetworkCountryEcpm();
        buildAdNetworkAdTypeCountryEcpm3d();
        buildAdNetworkEcpm3d();
        log.debug("build instance stat cache end, cost:{}", System.currentTimeMillis() - start);
    }

    void buildInstanceCountryEcpm(String name, int hourCount) {
        build(name, cfg.dir, out -> {
            LocalDateTime now = LocalDateTime.now(cfg.TZ);
            adNetworkDelay.forEach((adnId, delay) -> {
                LocalDateTime end = now.plusHours(-delay);
                String endDay = end.format(ISO_DATE);
                String startDay = end.plusHours(-hourCount).format(ISO_DATE);
                String sql = "select instance_id,country,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                        " from stat_adnetwork" +
                        " where day>='" + startDay + "'" +
                        "  and day<='" + endDay + "'" +
                        " and adn_id=" + adnId +
                        "  and country is not null" +
                        " group by instance_id,country" +
                        " having impr_sum>2000 and revenue_sum>0";
                jdbcTemplate.query(sql, rs -> {
                    BigDecimal impr = rs.getBigDecimal("impr_sum");
                    BigDecimal revenue = rs.getBigDecimal("revenue_sum");
                    out.writeDelimited(StatPB.InstanceCountryEcpm.newBuilder()
                            .setInstanceId(rs.getInt("instance_id"))
                            .setCountry(StringUtils.defaultString(rs.getString("country")))
                            .setEcpm(revenue.multiply(d1000).divide(impr, 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                            .build());
                });
            });
        });

    }

    //adn country 24小时数据
    void buildAdNetworkCountryEcpm() {
        build("stat_adn_country_ecpm24h", cfg.dir, out -> {
            LocalDateTime now = LocalDateTime.now(cfg.TZ);
            adNetworkDelay.forEach((adnId, delay) -> {
                LocalDateTime end = now.plusHours(-delay);
                String endDay = end.format(ISO_DATE);
                String startDay = end.plusHours(-24).format(ISO_DATE);
                String sql = "select adn_id,country,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                        " from stat_adnetwork" +
                        " where day>='" + startDay + "'" +
                        "  and day<='" + endDay + "'" +
                        " and adn_id=" + adnId +
                        "  and country is not null" +
                        " group by adn_id,country" +
                        " having impr_sum > 2000 and revenue_sum > 0";
                jdbcTemplate.query(sql, rs -> {
                    BigDecimal impr = rs.getBigDecimal("impr_sum");
                    BigDecimal revenue = rs.getBigDecimal("revenue_sum");
                    out.writeDelimited(StatPB.AdNetworkCountryEcpm.newBuilder()
                            .setAdnId(rs.getInt("adn_id"))
                            .setCountry(StringUtils.defaultString(rs.getString("country")))
                            .setEcpm(revenue.multiply(d1000).divide(impr, 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                            .build());
                });
            });
        });
    }

    void buildAdNetworkAdTypeCountryEcpm3d() {
        build("stat_adn_adtype_country_ecpm3d", cfg.dir, out -> {
            LocalDateTime now = LocalDateTime.now(cfg.TZ);
            adNetworkDelay.forEach((adnId, delay) -> {
                LocalDateTime end = now.plusHours(-delay);
                String endDay = end.format(ISO_DATE);
                String startDay = end.plusDays(-3).format(ISO_DATE);

                String sql = "select adn_id,ad_type,country,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                        " from stat_adnetwork" +
                        " where day>='" + startDay + "'" +
                        "  and day<='" + endDay + "'" +
                        " and adn_id=" + adnId +
                        " group by adn_id,ad_type,country" +
                        " having impr_sum > 2000 and revenue_sum > 0";
                jdbcTemplate.query(sql, rs -> {
                    BigDecimal impr = rs.getBigDecimal("impr_sum");
                    BigDecimal revenue = rs.getBigDecimal("revenue_sum");
                    out.writeDelimited(StatPB.AdNetworkAdTypeCountryEcpm.newBuilder()
                            .setMediationId(rs.getInt("adn_id"))
                            .setPlacementType(rs.getInt("ad_type"))
                            .setCountry(StringUtils.defaultString(rs.getString("country")))
                            .setEcpm(revenue.multiply(d1000).divide(impr, 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                            .build());
                });
            });
        });
    }

    void buildAdNetworkEcpm3d() {
        build("stat_adn_ecpm3d", cfg.dir, out -> {
            LocalDateTime now = LocalDateTime.now(cfg.TZ);
            adNetworkDelay.forEach((adnId, delay) -> {
                LocalDateTime end = now.plusHours(-delay);
                String endDay = end.format(ISO_DATE);
                String startDay = end.plusDays(-3).format(ISO_DATE);

                String sql = "select adn_id,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                        " from stat_adnetwork" +
                        " where day>='" + startDay + "'" +
                        "  and day<='" + endDay + "'" +
                        " and adn_id=" + adnId +
                        " group by adn_id" +
                        " having impr_sum > 2000 and revenue_sum > 0";
                jdbcTemplate.query(sql, rs -> {
                    BigDecimal impr = rs.getBigDecimal("impr_sum");
                    BigDecimal revenue = rs.getBigDecimal("revenue_sum");
                    out.writeDelimited(StatPB.AdNetworkEcpm.newBuilder()
                            .setAdnId(rs.getInt("adn_id"))
                            .setEcpm(revenue.multiply(d1000).divide(impr, 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                            .build());
                });
            });
        });
    }
}
