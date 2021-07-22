package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.util.MathUtil;
import com.adtiming.om.dtask.util.Util;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class GoogleUarService {
    private static final Logger log = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplateW;

    @Resource
    private AppConfig cfg;

    @Scheduled(cron = "0 0 11 * * *")
    public void topUarCalculate() {
        if (cfg.isDev()) {
            return;
        }
        log.info("topUarCalculate start");
        long start = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger();
        try {
            String sql = "select * from om_publisher_app where sdk_report_uar_regions is not null and status=1";
            List<Map<String, Object>> list = jdbcTemplateW.queryForList(sql);

            if (!list.isEmpty()) {
                Set<Integer> pubAppIds = list.stream().map(o -> Util.getInt(o, "id")).collect(Collectors.toSet());
                LocalDateTime now = LocalDateTime.now();
                String mysqlStartDay = now.plusDays(-7).format(DateTimeFormatter.ISO_DATE);
                String mysqlEndDay = now.format(DateTimeFormatter.ISO_DATE);
                sql = String.format("select day,pub_app_id,country,uar1,uar2,uar3,uar4,uar5 from stat_pub_app_country_uar " +
                                " where pub_app_id in (%s) and day>=? and day<? order by pub_app_id,country,day",
                        StringUtils.join(pubAppIds, ","));
                List<Map<String, Object>> uarData = jdbcTemplateW.queryForList(sql, mysqlStartDay, mysqlEndDay);
                Map<Integer, Map<String, TreeMap<String, Map<String, Object>>>> pubAppCountryDayUar = new HashMap<>();
                for (Map<String, Object> uar : uarData) {
                    pubAppCountryDayUar.computeIfAbsent(Util.getInt(uar, "pub_app_id"), k -> new HashMap<>())
                            .computeIfAbsent(Util.getString(uar, "country"), k -> new TreeMap<>())
                            .put(Util.getString(uar, "day"), uar);
                }
                jdbcTemplateW.update(String.format("delete from om_publisher_app_country_uar where pub_app_id in (%s) and day=?", StringUtils.join(pubAppIds, ",")), mysqlEndDay);
                String insertSql = "insert into om_publisher_app_country_uar(pub_app_id,country,day,uar1,uar2,uar3,uar4,uar5) values(?,?,?,?,?,?,?,?)";
                List<Object[]> insertParams = new ArrayList<>();
                String updateSql = "update om_publisher_app set sdk_report_uar_auto=? where id=?";
                List<Object[]> updateParams = new ArrayList<>();
                pubAppCountryDayUar.forEach((pubAppId, countryDayUar) -> {
                    Map<String, BigDecimal[]> countryUar = new HashMap<>();
                    countryDayUar.forEach((country, dayUar) -> {
                        List<BigDecimal> uar1s = new ArrayList<>();
                        List<BigDecimal> uar2s = new ArrayList<>();
                        List<BigDecimal> uar3s = new ArrayList<>();
                        List<BigDecimal> uar4s = new ArrayList<>();
                        List<BigDecimal> uar5s = new ArrayList<>();
                        dayUar.forEach((day, uar) -> {
                            uar1s.add(Util.getBigDecimal(uar, "uar1"));
                            uar2s.add(Util.getBigDecimal(uar, "uar2"));
                            uar3s.add(Util.getBigDecimal(uar, "uar3"));
                            uar4s.add(Util.getBigDecimal(uar, "uar4"));
                            uar5s.add(Util.getBigDecimal(uar, "uar5"));
                        });
                        float level = 0.6F;
                        BigDecimal smoothParam = new BigDecimal(String.valueOf(level));
                        BigDecimal predictUar1 = MathUtil.expSmoothing(smoothParam, uar1s.toArray(new BigDecimal[0]));
                        BigDecimal predictUar2 = MathUtil.expSmoothing(smoothParam, uar2s.toArray(new BigDecimal[0]));
                        BigDecimal predictUar3 = MathUtil.expSmoothing(smoothParam, uar3s.toArray(new BigDecimal[0]));
                        BigDecimal predictUar4 = MathUtil.expSmoothing(smoothParam, uar4s.toArray(new BigDecimal[0]));
                        BigDecimal predictUar5 = MathUtil.expSmoothing(smoothParam, uar5s.toArray(new BigDecimal[0]));
                        insertParams.add(new Object[]{pubAppId, country, mysqlEndDay, predictUar1, predictUar2,
                                predictUar3, predictUar4, predictUar5});
                        count.getAndIncrement();
                        if (insertParams.size() >= 1000) {
                            jdbcTemplateW.batchUpdate(insertSql, insertParams);
                            insertParams.clear();
                        }
                        countryUar.put(country, new BigDecimal[]{predictUar1, predictUar2, predictUar3, predictUar4, predictUar5});
                    });
                    updateParams.add(new Object[]{JSON.toJSONString(countryUar), pubAppId});
                    if (updateParams.size() >= 1000) {
                        jdbcTemplateW.batchUpdate(updateSql, updateParams);
                        updateParams.clear();
                    }
                });
                if (insertParams.size() > 0) {
                    jdbcTemplateW.batchUpdate(insertSql, insertParams);
                    insertParams.clear();
                }
                if (updateParams.size() > 0) {
                    jdbcTemplateW.batchUpdate(updateSql, updateParams);
                }
            }
        } catch (Exception e) {
            log.error("topUarCalculate error", e);
        }
        log.info("topUarCalculate end, count:{}, cost:{}", count.get(), System.currentTimeMillis() - start);
    }
}
