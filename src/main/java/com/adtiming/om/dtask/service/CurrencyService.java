// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by huangqiang on 2019/6/19.
 * CurrencyService
 */
@Service
public class CurrencyService {

    private static Logger log = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Resource
    private HttpClient httpClient;

    private Map<String, BigDecimal> getCurrencyExchanges() {
        String url = String.format("http://data.fixer.io/api/latest?access_key=%s", cfg.currencyApiKey);
        HttpEntity entity = null;
        try {
            HttpGet req = new HttpGet(url);
            req.setConfig(RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(10000).build());
            HttpResponse res = httpClient.execute(req);
            String result = EntityUtils.toString(entity = res.getEntity(), StandardCharsets.UTF_8);
            System.out.println(result);
            if (StringUtils.isNotBlank(result)) {
                JSONObject json = JSONObject.parseObject(result);
                boolean success = json.getBoolean("success");
                if (success) {
                    JSONObject exhanges = json.getJSONObject("rates");
                    BigDecimal usdRate = exhanges.getBigDecimal("USD");
                    Map<String, BigDecimal> exchangeMap = new HashMap<>();
                    //base currency is EUR
                    //String base = json.getString("base");
                    BigDecimal baseOne = new BigDecimal(1);
                    for (String cur : exhanges.keySet()) {
                        BigDecimal rate = exhanges.getBigDecimal(cur);
                        if ("USD".equals(cur)) {
                            exchangeMap.put("USD", baseOne);
                            continue;
                        }

                        BigDecimal changeRate = baseOne.multiply(usdRate).divide(rate, 6, RoundingMode.HALF_UP);
                        exchangeMap.put(cur, changeRate);
                    }
                    return exchangeMap;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        return null;
    }

    @Scheduled(cron = "0 0 0 * * *")
    public void refreshCurrency2D() {
        if (!cfg.isProd())
            return;
        long start = System.currentTimeMillis();
        log.info("refreshCurrency2D start");
        try {
            Map<String, BigDecimal> exchangeMap = getCurrencyExchanges();
            if (exchangeMap != null) {
                List<Object[]> insertParam = new ArrayList<>(exchangeMap.size());
                List<Object[]> insertUpdateParam = new ArrayList<>(exchangeMap.size());
                String day = LocalDateTime.now(cfg.TZ).format(cfg.LOG_DATE_FORMAT);
                exchangeMap.forEach((k, v) -> {
                    log.info("from:{},rate:{}", k, v);
                    insertParam.add(new Object[]{day, k, "USD", v});
                    insertUpdateParam.add(new Object[]{k, "USD", v, v});
                });
                if (!insertParam.isEmpty()) {
                    jdbcTemplate.batchUpdate("insert into om_currency_exchange_day(`day`,cur_from,cur_to,exchange_rate)VALUES(?,?,?,?)", insertParam);
                }
                if (!insertUpdateParam.isEmpty()) {
                    jdbcTemplate.batchUpdate("insert into om_currency_exchange(cur_from,cur_to,exchange_rate)VALUES(?,?,?) ON DUPLICATE KEY UPDATE exchange_rate=?", insertUpdateParam);
                }
            }
        } catch (Exception e) {
            log.error("refreshCurrency2D error", e);
        }
        log.info("refreshCurrency2D finished, cost:{}", System.currentTimeMillis() - start);
    }
}
