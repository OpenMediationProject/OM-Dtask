// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import com.adtiming.om.pb.StatPB;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import static java.time.format.DateTimeFormatter.ISO_DATE;

/**
 * Created by huangqiang on 2019/11/12.
 * SDKInstanceStatBuilder
 */
@Service
public class StatInstanceBuilder extends PbBuiler {

    private static final Logger log = LogManager.getLogger();

    private static final DateTimeFormatter YMD = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final BigDecimal D1000 = new BigDecimal("1000");

    @Resource
    private AppConfig cfg;

    @Resource
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;

    @Resource
    private JdbcTemplate jdbcTemplate;

    //AdNetwork api report delay in hours
    private final Map<Integer, Integer> adNetworkDelay = new HashMap<>(20);

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
        if (cfg.isDev())
            return;
        threadPoolTaskScheduler.getScheduledExecutor().schedule(this::buildCache, 30, TimeUnit.SECONDS);
    }

    @Scheduled(cron = "0 0 * * * ?")
    private void buildCache() {
        if (cfg.isDev())
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

        buildExponentialSmoothingData();
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

    // Build ExponentialSmoothing cache data
    private void buildExponentialSmoothingData() {
        buildInstanceCountry7DayEcpm();
        buildAdnCountryAdTypeEcpm3d();
        buildCountryAdTypeEcpm3d();
        buildCountryEcpm3d();
        buildAdnAdTypeEcpm3d();
        buildAdTypeEcpm3d();
        buildAdnEcpm3d();
    }

    // For ExponentialSmoothing instance+country+7d
    private void buildInstanceCountry7DayEcpm() {
        String name = "instance_country_ecpm_7d";
        log.debug("start to build {}", name);
        long start = System.currentTimeMillis();

        File src = new File(cfg.dir, name + ".gz.tmp");
        File dst = new File(cfg.dir, name + ".gz");

        try (OutputStream out = new GZIPOutputStream(new FileOutputStream(src))) {
            LocalDateTime now = LocalDateTime.now();
            Map<Integer, Map<String, Map<String, BigDecimal[]>>> data = new HashMap<>();
            adNetworkDelay.forEach((adnId, delay) -> {
                String mysql_start_day = now.plusHours(-delay).plusDays(-7).format(YMD);
                String mysql_end_day = now.plusHours(-delay).format(YMD);
                String sql = "select day,instance_id,country,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                        " from stat_adnetwork" +
                        " where day>='" + mysql_start_day + "'" +
                        "  and day<='" + mysql_end_day + "'" +
                        " and adn_id=" + adnId +
                        "  and country is not null" +
                        " group by day,instance_id,country" +
                        " having impr_sum>0";
                jdbcTemplate.query(sql, rs -> {
                    BigDecimal impr = rs.getBigDecimal("impr_sum");
                    BigDecimal revenue = rs.getBigDecimal("revenue_sum");
                    data.computeIfAbsent(rs.getInt("instance_id"), k -> new HashMap<>())
                            .computeIfAbsent(rs.getString("country"), k -> new HashMap<>())
                            .put(rs.getString("day"), new BigDecimal[]{impr, revenue});
                });
            });
            
            data.forEach((pid, cic) -> cic.forEach((country, dic) -> dic.forEach((d, ic) -> {
                try {
                    StatPB.InstanceCountryDayEcpm.newBuilder()
                            .setInstanceId(pid)
                            .setCountry(StringUtils.defaultString(country))
                            .setDay(d)
                            .setImpr(ic[0].intValue())
                            .setCost(ic[1].floatValue())
                            .build()
                            .writeDelimitedTo(out);
                } catch (IOException e) {
                    log.error("write error", e);
                }
            })));

            out.close();
            if (!src.renameTo(dst)) {
                log.error("mv {} to {} error", src, dst);
            }
        } catch (Exception e) {
            log.error("build {} error", name, e);
        }

        log.debug("end to build {}, cost: {} ms", name, System.currentTimeMillis() - start);
    }

    // For ExponentialSmoothing Adn+country+adType+3d
    private void buildAdnCountryAdTypeEcpm3d() {
        String name = "adn_adtype_country_ecpm_3d";
        log.debug("start to build {}", name);
        long start = System.currentTimeMillis();

        File src = new File(cfg.dir, name + ".gz.tmp");
        File dst = new File(cfg.dir, name + ".gz");

        try (OutputStream out = new GZIPOutputStream(new FileOutputStream(src))) {
            LocalDateTime now = LocalDateTime.now();
            Map<String, BigDecimal[]> data = new HashMap<>();
            adNetworkDelay.forEach((adnId, delay) -> {
                String mysql_start_day = now.plusHours(-delay).plusDays(-3).format(YMD);
                String mysql_end_day = now.plusHours(-delay).format(YMD);
                String sql = "select day,adn_id,ad_type,country,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                        " from stat_adnetwork" +
                        " where day>='" + mysql_start_day + "'" +
                        "  and day<='" + mysql_end_day + "'" +
                        " and adn_id=" + adnId +
                        "  and country is not null" +
                        " group by day,adn_id,ad_type,country" +
                        " having impr_sum>0";
                jdbcTemplate.query(sql, rs -> {
                    BigDecimal impr = rs.getBigDecimal("impr_sum");
                    BigDecimal revenue = rs.getBigDecimal("revenue_sum");
                    String key = String.format("%d_%d_%s", rs.getInt("adn_id"),
                            rs.getInt("ad_type"),
                            rs.getString("country"));
                    data.put(key, new BigDecimal[]{impr, revenue});
                });
            });
            data.forEach((key, ic) -> {
                try {
                    String[] kv = key.split("_");
                    int adnId = NumberUtils.toInt(kv[0]);
                    int pt = NumberUtils.toInt(kv[1]);
                    String country = kv[2];
                    StatPB.AdnCountryAdTypeEcpm.newBuilder()
                            .setAdnId(adnId)
                            .setAdType(pt)
                            .setCountry(country)
                            .setEcpm(ic[1].multiply(D1000).divide(ic[0], 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                            .build()
                            .writeDelimitedTo(out);
                } catch (IOException e) {
                    log.error("write error", e);
                }
            });

            out.close();
            if (!src.renameTo(dst)) {
                log.error("mv {} to {} error", src, dst);
            }
        } catch (Exception e) {
            log.error("build {} error", name, e);
        }

        log.debug("end to build {}, cost: {} ms", name, System.currentTimeMillis() - start);
    }

    // For ExponentialSmoothing country+adType+3d
    private void buildCountryAdTypeEcpm3d() {
        String name = "adtype_country_ecpm_3d";
        log.debug("start to build {}", name);
        long start = System.currentTimeMillis();

        File src = new File(cfg.dir, name + ".gz.tmp");
        File dst = new File(cfg.dir, name + ".gz");

        try (OutputStream out = new GZIPOutputStream(new FileOutputStream(src))) {
            LocalDateTime now = LocalDateTime.now();
            Map<Integer, Map<String, BigDecimal[]>> data = new HashMap<>();
            String mysql_start_day = now.plusDays(-3).format(YMD);
            String mysql_end_day = now.format(YMD);
            String sql = "select day,ad_type,country,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                    " from stat_adnetwork" +
                    " where day>='" + mysql_start_day + "'" +
                    "  and day<='" + mysql_end_day + "'" +
                    "  and country is not null" +
                    " group by day,ad_type,country" +
                    " having impr_sum>0";

            jdbcTemplate.query(sql, rs -> {
                BigDecimal impr = rs.getBigDecimal("impr_sum");
                BigDecimal revenue = rs.getBigDecimal("revenue_sum");
                BigDecimal[] b = data.computeIfAbsent(rs.getInt("ad_type"), k -> new HashMap<>())
                        .computeIfAbsent(rs.getString("country"), k -> new BigDecimal[2]);
                b[0] = b[0] == null ? impr : b[0].add(impr);
                b[1] = b[1] == null ? revenue : b[1].add(revenue);
            });
            data.forEach((adType, ce) -> ce.forEach((country, ic) -> {
                try {
                    StatPB.CountryAdTypeEcpm.newBuilder()
                            .setAdType(adType)
                            .setCountry(country)
                            .setEcpm(ic[1].multiply(D1000).divide(ic[0], 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                            .build()
                            .writeDelimitedTo(out);
                } catch (IOException e) {
                    log.error("write error", e);
                }
            }));

            out.close();
            if (!src.renameTo(dst)) {
                log.error("mv {} to {} error", src, dst);
            }
        } catch (Exception e) {
            log.error("build {} error", name, e);
        }

        log.debug("end to build {}, cost: {} ms", name, System.currentTimeMillis() - start);
    }

    // For ExponentialSmoothing country+3d
    private void buildCountryEcpm3d() {
        String name = "country_ecpm_3d";
        log.debug("start to build {}", name);
        long start = System.currentTimeMillis();

        File src = new File(cfg.dir, name + ".gz.tmp");
        File dst = new File(cfg.dir, name + ".gz");

        try (OutputStream out = new GZIPOutputStream(new FileOutputStream(src))) {
            LocalDateTime now = LocalDateTime.now();
            Map<String, BigDecimal[]> data = new HashMap<>();
            String mysql_start_day = now.plusDays(-3).format(YMD);
            String mysql_end_day = now.format(YMD);
            String sql = "select day,country,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                    " from stat_adnetwork" +
                    " where day>='" + mysql_start_day + "'" +
                    "  and day<='" + mysql_end_day + "'" +
                    "  and country is not null" +
                    " group by day,country" +
                    " having impr_sum>0";
            jdbcTemplate.query(sql, rs -> {
                BigDecimal impr = rs.getBigDecimal("impr_sum");
                BigDecimal revenue = rs.getBigDecimal("revenue_sum");
                BigDecimal[] b = data.computeIfAbsent(rs.getString("country"), k -> new BigDecimal[2]);
                b[0] = b[0] == null ? impr : b[0].add(impr);
                b[1] = b[1] == null ? revenue : b[1].add(revenue);
            });

            data.forEach((country, ic) -> {
                try {
                    StatPB.CountryEcpm.newBuilder()
                            .setCountry(country)
                            .setEcpm(ic[1].multiply(D1000).divide(ic[0], 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                            .build()
                            .writeDelimitedTo(out);
                } catch (IOException e) {
                    log.error("write error", e);
                }
            });

            out.close();
            if (!src.renameTo(dst)) {
                log.error("mv {} to {} error", src, dst);
            }
        } catch (Exception e) {
            log.error("build {} error", name, e);
        }

        log.debug("end to build {}, cost: {} ms", name, System.currentTimeMillis() - start);
    }

    // For ExponentialSmoothing adType+3d
    private void buildAdTypeEcpm3d() {
        String name = "adtype_ecpm_3d";
        log.debug("start to build {}", name);
        long start = System.currentTimeMillis();

        File src = new File(cfg.dir, name + ".gz.tmp");
        File dst = new File(cfg.dir, name + ".gz");

        try (OutputStream out = new GZIPOutputStream(new FileOutputStream(src))) {
            LocalDateTime now = LocalDateTime.now();
            Map<Integer, BigDecimal[]> data = new HashMap<>();
            String mysql_start_day = now.plusDays(-3).format(YMD);
            String mysql_end_day = now.format(YMD);
            String sql = "select day,ad_type,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                    " from stat_adnetwork" +
                    " where day>='" + mysql_start_day + "'" +
                    "  and day<='" + mysql_end_day + "'" +
                    "  and country is not null" +
                    " group by day,ad_type" +
                    " having impr_sum>0";
            jdbcTemplate.query(sql, rs -> {
                BigDecimal impr = rs.getBigDecimal("impr_sum");
                BigDecimal revenue = rs.getBigDecimal("revenue_sum");
                BigDecimal[] b = data.computeIfAbsent(rs.getInt("ad_type"), k -> new BigDecimal[2]);
                b[0] = b[0] == null ? impr : b[0].add(impr);
                b[1] = b[1] == null ? revenue : b[1].add(revenue);
            });

            data.forEach((adType, ic) -> {
                try {
                    StatPB.AdTypeEcpm.newBuilder()
                            .setAdType(adType)
                            .setEcpm(ic[1].multiply(D1000).divide(ic[0], 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                            .build()
                            .writeDelimitedTo(out);
                } catch (IOException e) {
                    log.error("write error", e);
                }
            });

            out.close();
            if (!src.renameTo(dst)) {
                log.error("mv {} to {} error", src, dst);
            }
        } catch (Exception e) {
            log.error("build {} error", name, e);
        }

        log.debug("end to build {}, cost: {} ms", name, System.currentTimeMillis() - start);
    }

    // For ExponentialSmoothing Adn+adType+3d
    private void buildAdnAdTypeEcpm3d() {
        String name = "adn_adtype_ecpm_3d";
        log.debug("start to build {}", name);
        long start = System.currentTimeMillis();

        File src = new File(cfg.dir, name + ".gz.tmp");
        File dst = new File(cfg.dir, name + ".gz");

        try (OutputStream out = new GZIPOutputStream(new FileOutputStream(src))) {
            LocalDateTime now = LocalDateTime.now();
            Map<String, BigDecimal[]> data = new HashMap<>();
            adNetworkDelay.forEach((adnId, delay) -> {
                String mysql_start_day = now.plusHours(-delay).plusDays(-3).format(YMD);
                String mysql_end_day = now.plusHours(-delay).format(YMD);
                String sql = "select day,adn_id,ad_type,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                        " from stat_adnetwork" +
                        " where day>='" + mysql_start_day + "'" +
                        "  and day<='" + mysql_end_day + "'" +
                        " and adn_id=" + adnId +
                        "  and country is not null" +
                        " group by day,adn_id,ad_type" +
                        " having impr_sum>0";
                jdbcTemplate.query(sql, rs -> {
                    BigDecimal impr = rs.getBigDecimal("impr_sum");
                    BigDecimal revenue = rs.getBigDecimal("revenue_sum");
                    String key = String.format("%d_%d", rs.getInt("adn_id"),
                            rs.getInt("ad_type"));
                    data.put(key, new BigDecimal[]{impr, revenue});
                });
            });

            data.forEach((key, ic) -> {
                try {
                    String[] kv = key.split("_");
                    int adnId = NumberUtils.toInt(kv[0]);
                    int pt = NumberUtils.toInt(kv[1]);
                    StatPB.AdnAdTypeEcpm.newBuilder()
                            .setAdnId(adnId)
                            .setAdType(pt)
                            .setEcpm(ic[1].multiply(D1000).divide(ic[0], 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                            .build()
                            .writeDelimitedTo(out);
                } catch (IOException e) {
                    log.error("write error", e);
                }
            });

            out.close();
            if (!src.renameTo(dst)) {
                log.error("mv {} to {} error", src, dst);
            }
        } catch (Exception e) {
            log.error("build {} error", name, e);
        }

        log.debug("end to build {}, cost: {} ms", name, System.currentTimeMillis() - start);
    }

    // For ExponentialSmoothing Adn+3d
    private void buildAdnEcpm3d() {
        String name = "adn_ecpm_3d";
        log.debug("start to build {}", name);
        long start = System.currentTimeMillis();

        File src = new File(cfg.dir, name + ".gz.tmp");
        File dst = new File(cfg.dir, name + ".gz");

        try (OutputStream out = new GZIPOutputStream(new FileOutputStream(src))) {
            LocalDateTime now = LocalDateTime.now();
            Map<Integer, BigDecimal[]> data = new HashMap<>();
            adNetworkDelay.forEach((adnId, delay) -> {
                String mysql_start_day = now.plusHours(-delay).plusDays(-3).format(YMD);
                String mysql_end_day = now.plusHours(-delay).format(YMD);
                String sql = "select day,adn_id,sum(api_impr) impr_sum,sum(revenue) revenue_sum" +
                        " from stat_adnetwork" +
                        " where day>='" + mysql_start_day + "'" +
                        "  and day<='" + mysql_end_day + "'" +
                        " and adn_id=" + adnId +
                        "  and country is not null" +
                        " group by day,adn_id" +
                        " having impr_sum>0";
                jdbcTemplate.query(sql, rs -> {
                    BigDecimal impr = rs.getBigDecimal("impr_sum");
                    BigDecimal revenue = rs.getBigDecimal("revenue_sum");
                    data.put(rs.getInt("adn_id"), new BigDecimal[]{impr, revenue});
                });
            });

            data.forEach((adnId, ic) -> {
                try {
                    StatPB.AdnEcpm.newBuilder()
                            .setAdnId(adnId)
                            .setEcpm(ic[1].multiply(D1000).divide(ic[0], 6, BigDecimal.ROUND_HALF_DOWN).floatValue())
                            .build()
                            .writeDelimitedTo(out);
                } catch (IOException e) {
                    log.error("write error", e);
                }
            });

            out.close();
            if (!src.renameTo(dst)) {
                log.error("mv {} to {} error", src, dst);
            }
        } catch (Exception e) {
            log.error("build {} error", name, e);
        }

        log.debug("end to build {}, cost: {} ms", name, System.currentTimeMillis() - start);
    }
}
