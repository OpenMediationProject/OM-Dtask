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
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class StatBuilder extends PbBuiler {

    private static final Logger log = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @PostConstruct
    private void init() {
        if (!cfg.isProd())
            return;
        threadPoolTaskScheduler.getScheduledExecutor().schedule(this::buildCache, 30, TimeUnit.SECONDS);
    }

    @Scheduled(cron = "0 0 10 * * ?")
    private void buildCache() {
        if (!cfg.isProd())
            return;
        log.debug("start to build cache");
        long start = System.currentTimeMillis();
        buildPlacementCountryEcpm();
        buildInstanceCountryEcpm();
        // for Placement Default FloorPrice
        buildPlacementEcpm();
        buildCountryAdTypeEcpm();

        log.debug("build cache finished, cost {} ms", System.currentTimeMillis() - start);
    }

    void buildInstanceCountryEcpm() {
        build("stat_instance_country_ecpm", cfg.dir, out -> {
            LocalDateTime now = LocalDateTime.now();
            String startDay = now.plusDays(-3).format(DateTimeFormatter.ISO_DATE);
            String endDay = now.format(DateTimeFormatter.ISO_DATE);

            String sql = "select instance_id,country,sum(api_impr) impr_sum,sum(cost) cost_sum" +
                    " from stat_adnetwork" +
                    " where day>='" + startDay + "'" +
                    "  and day<='" + endDay + "'" +
                    "  and country is not null" +
                    " group by instance_id,country" +
                    " having impr_sum>0 and cost_sum>0";
            jdbcTemplate.query(sql, rs -> {
                BigDecimal impr = rs.getBigDecimal("impr_sum");
                BigDecimal cost = rs.getBigDecimal("cost_sum");
                out.writeDelimited(StatPB.InstanceCountryEcpm.newBuilder()
                        .setInstanceId(rs.getInt("instance_id"))
                        .setCountry(StringUtils.defaultString(rs.getString("country")))
                        .setEcpm(cost.multiply(d1000).divide(impr, 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                        .build());
            });
        });

    }

    void buildPlacementCountryEcpm() {
        build("stat_placement_country_ecpm", cfg.dir, out -> {
            LocalDateTime now = LocalDateTime.now();
            String mysqlStartDay = now.plusDays(-3).format(DateTimeFormatter.ISO_DATE);
            String mysqlEndDay = now.format(DateTimeFormatter.ISO_DATE);

            Map<Integer, Map<String, BigDecimal[]>> data = new HashMap<>();
            String sql = "select placement_id,country,sum(api_impr) impr_sum,sum(cost) cost_sum" +
                    " from stat_adnetwork" +
                    " where day>='" + mysqlStartDay + "'" +
                    "  and day<='" + mysqlEndDay + "'" +
                    "  and country is not null" +
                    " group by placement_id,country" +
                    " having impr_sum>0 and cost_sum>0";
            jdbcTemplate.query(sql, rs -> {
                BigDecimal impr = rs.getBigDecimal("impr_sum");
                BigDecimal cost = rs.getBigDecimal("cost_sum");
                data.computeIfAbsent(rs.getInt("placement_id"), k -> new HashMap<>())
                        .put(rs.getString("country"), new BigDecimal[]{impr, cost});
            });

            data.forEach((pid, cic) -> cic.forEach((country, ic) -> {
                out.writeDelimited(StatPB.PlacementCountryEcpm.newBuilder()
                        .setPlacementId(pid)
                        .setCountry(StringUtils.defaultString(country))
                        .setEcpm(ic[1].multiply(d1000).divide(ic[0], 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                        .build());
            }));

        });
    }

    /**
     * For Placement FloorPrice fix<br>
     * 取Placement eCPM, Country+Placement, imp>2000, 最近7天
     */
    void buildPlacementEcpm() {
        build("stat_placement_ecpm", cfg.dir, out -> {
            LocalDate nowDate = LocalDate.now();
            LocalDate startDate = nowDate.plusDays(-7);

            String sql = "select placement_id,country,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                    " from stat_adnetwork" +
                    " where day >= ? and day <= ? and country is not null" +
                    " group by placement_id,country" +
                    " having impr_sum > 2000";
            BigDecimal d1000 = new BigDecimal(1000);
            jdbcTemplate.query(sql, rs -> {
                BigDecimal ecpm = rs.getBigDecimal("revenue_sum").multiply(d1000)
                        .divide(rs.getBigDecimal("impr_sum"), 6, RoundingMode.HALF_UP);
                out.writeDelimited(StatPB.PlacementEcpm.newBuilder()
                        .setPlacementId(rs.getInt("placement_id"))
                        .setCountry(rs.getString("country"))
                        .setEcpm(ecpm.floatValue())
                        .build());
            }, startDate, nowDate);
        });
    }

    /**
     * For Placement FloorPrice fix
     * 取全局Country+AdType 平均eCPM
     */
    void buildCountryAdTypeEcpm() {
        build("stat_country_adtype_ecpm", cfg.dir, out -> {
            LocalDate nowDate = LocalDate.now();
            LocalDate startDate = nowDate.plusDays(-7);

            String sql = "select a.country,a.ad_type,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                    " from stat_adnetwork a" +
                    " where a.day >= ?" +
                    "   and a.day <= ?" +
                    "   and a.country is not null" +
                    " group by a.country,a.ad_type" +
                    " having impr_sum > 0";
            BigDecimal d1000 = new BigDecimal(1000);
            jdbcTemplate.query(sql, rs -> {
                BigDecimal ecpm = rs.getBigDecimal("revenue_sum").multiply(d1000)
                        .divide(rs.getBigDecimal("impr_sum"), 6, RoundingMode.HALF_UP);
                out.writeDelimited(StatPB.CountryAdTypeEcpm.newBuilder()
                        .setAdType(rs.getInt("ad_type"))
                        .setCountry(rs.getString("country"))
                        .setEcpm(ecpm.floatValue())
                        .build());
            }, startDate, nowDate);
        });

    }

}
