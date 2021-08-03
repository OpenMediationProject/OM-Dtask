// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask;

import com.adtiming.om.dtask.cloud.CloudClient;
import com.adtiming.om.dtask.util.JSONRowMapper;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.ClassUtils;
import org.springframework.web.context.support.GenericWebApplicationContext;

@SpringBootApplication(scanBasePackages = {"com.adtiming.om"})
@EnableScheduling
public class Application {

    private static final Logger log = LogManager.getLogger();

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Scheduled(cron = "0 1 * * * ?")
    private void gzipAccessLogHourly() {
        log.info("gzip access log start");
        try {
            String[] cmd = {"bash", "-c", "cd log; ls access.*.log|xargs gzip"};
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            IOUtils.copy(p.getInputStream(), System.out);
            log.info("gzip access log finished {}", p.waitFor());
        } catch (Exception e) {
            log.error("gzip access log error", e);
        }
    }

    @Bean(destroyMethod = "close")
    public CloseableHttpClient httpClient() {
        return HttpClients.custom()
                .setMaxConnPerRoute(50000)
                .setMaxConnTotal(100000)
                .setUserAgent("om-dtask")
                .build();
    }

    @Bean
    public CloudClient cloudClient(@Autowired JdbcTemplate jdbcTemplate,
                                   @Autowired GenericWebApplicationContext context) throws Exception {
        String sql = "SELECT cloud_type,cloud_config FROM om_server_dcenter WHERE cloud_type!='' limit 1";
        try {
            JSONObject o = jdbcTemplate.queryForObject(sql, JSONRowMapper.getCamelInstance());
            assert o != null;
            String cloudType = o.getString("cloudType");
            String cloudConfig = o.getString("cloudConfig");
            // dynamic load class to avoid unnecessary memory cost
            String namePrefix = StringUtils.capitalize(cloudType);
            String className = "com.adtiming.om.dtask.cloud." + cloudType + "." + namePrefix + "CloudClient";
            Class<?> clazz = ClassUtils.forName(className, null);
            CloudClient client = (CloudClient) clazz.getConstructor(String.class).newInstance(cloudConfig);
            client.registerBeans(context);
            return client;
        } catch (DataAccessException e) {
            return CloudClient.CLIENT0;
        }
    }

}
