// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

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

    @Value("${currency.api.appkey}")
    public String appKey;
    public static final String URL = "http://op.juhe.cn/onebox/exchange/currency";
    public static final String to = "USD";

    public float getCurrency(String from, String to) {
        HttpGet req = new HttpGet(String.format("%s?key=%s&from=%s&to=%s", URL, appKey, from, to));
        req.setConfig(RequestConfig.custom().setSocketTimeout(3000).build());
        HttpEntity entity = null;
        try {
            HttpResponse res = httpClient.execute(req);
            entity = res.getEntity();
            String result = EntityUtils.toString(entity);
            JSONObject json = JSONObject.parseObject(result);

            if (json.getIntValue("error_code") != 0) {
                throw new Exception(json.getString("reason"));
            } else {
                float exchange = -1;
                for (Object j : json.getJSONArray("result")) {
                    JSONObject jsonObject = (JSONObject) j;
                    if (from.equalsIgnoreCase(jsonObject.getString("currencyF")) && to.equalsIgnoreCase(jsonObject.getString("currencyT"))) {
                        exchange = jsonObject.getFloatValue("result");
                        break;
                    }
                }
                return exchange;
            }
        } catch (HttpException | IOException e) {
            log.warn("io error", e);
        } catch (Exception e) {
            log.error("get currency error", e);
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        return 0F;
    }

    @Scheduled(cron = "0 0 0 * * *")
    public void runCurrencyExchange() {
        if (!cfg.isProd())
            return;
        long start = System.currentTimeMillis();
        log.info("runCurrencyExchange start");
        try {
            String sql = "select cur_from from om_currency_exchange";
            List<String> list = jdbcTemplate.queryForList(sql, String.class);
            if (list.isEmpty())
                return;
            List<Object[]> insertParam = new ArrayList<>(list.size());
            List<Object[]> insertUpdateParam = new ArrayList<>(list.size());
            String day = LocalDateTime.now(cfg.TZ).format(cfg.LOG_DATE_FORMAT);
            for (String from : list) {
                try {
                    float c = getCurrency(from, "USD");
                    if (c > 0F) {
                        log.info("from:{},rate:{}", from, c);
                        insertParam.add(new Object[]{day, from, "USD", c});
                        insertUpdateParam.add(new Object[]{from, "USD", c, c});
                    }
                } catch (Exception e) {
                    log.warn("getCurrency error,from:{}, {}", from, e.toString());
                }
            }
            if (!insertParam.isEmpty()) {
                jdbcTemplate.batchUpdate("insert into om_currency_exchange_day(`day`,cur_from,cur_to,exchange_rate)VALUES(?,?,?,?)", insertParam);
            }
            if (!insertUpdateParam.isEmpty()) {
                jdbcTemplate.batchUpdate("insert into om_currency_exchange(cur_from,cur_to,exchange_rate)VALUES(?,?,?) ON DUPLICATE KEY UPDATE exchange_rate=?", insertUpdateParam);
            }
        } catch (Exception e) {
            log.error("runCurrencyExchange error", e);
        }
        log.info("runCurrencyExchange finished, cost:{}", System.currentTimeMillis() - start);
    }
}
