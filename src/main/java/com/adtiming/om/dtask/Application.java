// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask;

import com.adtiming.om.dtask.aws.AwsConfig;
import org.apache.commons.io.IOUtils;
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
    public AwsConfig awsConfig(@Autowired JdbcTemplate jdbcTemplate) {
        String sql = "select id,name,s3_status,s3_region,s3_bucket,s3_access_key_id,s3_secret_access_key" +
                " from om_server_dcenter where s3_status=1 limit 1";
        try {
            return jdbcTemplate.queryForObject(sql, AwsConfig.ROWMAPPER);
        } catch (DataAccessException e) {
            return new AwsConfig();
        }
    }

}
