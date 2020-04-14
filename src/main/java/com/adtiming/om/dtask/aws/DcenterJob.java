// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.aws;

import com.adtiming.om.dtask.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;

@Service
public class DcenterJob {
    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private TemplateEngine templateEngine;

    @Resource
    private AwsConfig awsConfig;

    @Value("${aws.athena.database}")
    private String athenaDatabase;

    @Value("${aws.athena.data-dir}")
    private String dataDirectory;

    @Resource
    private S3Executor s3Executor;

    @Resource
    private AthenaExecutor athenaExecutor;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @PostConstruct
    private void init() {
        if (awsConfig.isDisabled()) {
            return;
        }
        LocalDateTime executeDateTime = LocalDateTime.now();
        String year = executeDateTime.format(Constants.FORMATTER_YYYY);
        String month = executeDateTime.format(Constants.FORMATTER_MM);
        String day = executeDateTime.format(Constants.FORMATTER_DD);
        String resultS3Directory = String.join("/", "s3:/", awsConfig.getS3Bucket(), AthenaConstants.DATA_PATH_ATHENA, year, month, day);
        new Thread(() -> {
            initDatabase(resultS3Directory);
            initTable(resultS3Directory);
        }, "s3Init").start();
    }

    private void initDatabase(String resultS3Directory) {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("database", athenaDatabase);
        String initSql = this.templateEngine.process("init_database", new Context(null, valueMap));
        LOG.debug("init database sql:\n {}", initSql);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, initSql, resultS3Directory);
        LOG.info("init database start, id: {}", queryExecutionId);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("init database complete id: {}, result path: {} ", queryExecutionId, resultS3Directory);
    }

    private void initTable(String resultS3Directory) {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("s3Bucket", awsConfig.getS3Bucket());
        valueMap.put("tableDataPath", AthenaConstants.DATA_PATH_TABLE);
        valueMap.put("tableName", AthenaConstants.TABLE_NAME_LR);
        String initSql = this.templateEngine.process("init_table", new Context(null, valueMap));
        LOG.debug("init table sql:\n {}", initSql);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, initSql, resultS3Directory);
        LOG.info("init table start, id: {}", queryExecutionId);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("init table complete id: {}, result path: {} ", queryExecutionId, resultS3Directory);
    }

    public void addPartition(LocalDateTime executeDateTime) {
        // 增加分区
        String year = executeDateTime.format(Constants.FORMATTER_YYYY);
        String month = executeDateTime.format(Constants.FORMATTER_MM);
        String day = executeDateTime.format(Constants.FORMATTER_DD);
        String hour = executeDateTime.format(Constants.FORMATTER_HH);

        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        valueMap.put("hour", hour);
        valueMap.put("s3Bucket", awsConfig.getS3Bucket());
        valueMap.put("tableDataPath", AthenaConstants.DATA_PATH_TABLE);
        valueMap.put("tableName", AthenaConstants.TABLE_NAME_LR);

        // 渲染 SQL
        String partitionSql = this.templateEngine.process("add_partition", new Context(null, valueMap));
        LOG.debug("add partition sql:\n {}", partitionSql);
        // 执行 SQL
        String resultS3Directory = String.join("/", "s3:/", awsConfig.getS3Bucket(), AthenaConstants.DATA_PATH_ATHENA, year, month, day);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, partitionSql, resultS3Directory);
        LOG.info("add partition start id: {}", queryExecutionId);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("add partition complete id: {}, result path: {} ", queryExecutionId, resultS3Directory);
    }

    public void commonReport(LocalDateTime executeDateTime) {
        String year = executeDateTime.format(Constants.FORMATTER_YYYY);
        String month = executeDateTime.format(Constants.FORMATTER_MM);
        String day = executeDateTime.format(Constants.FORMATTER_DD);
        String hour = executeDateTime.format(Constants.FORMATTER_HH);

        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        valueMap.put("hour", hour);
        valueMap.put("tableName", AthenaConstants.TABLE_NAME_LR);

        String reportSql = this.templateEngine.process("report_common", new Context(null, valueMap));
        LOG.info("report common, sql:\n {}", reportSql);

        String resultS3Directory = String.join("/", "s3:/", awsConfig.getS3Bucket(), AthenaConstants.DATA_PATH_ATHENA, year, month, day);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, reportSql, resultS3Directory);
        LOG.info("report common query start...  id: {}", queryExecutionId);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("report common query complete, id: {}, result directory: {} ", queryExecutionId, resultS3Directory);

        String resultS3Key = String.join("/", AthenaConstants.DATA_PATH_ATHENA, year, month, day, queryExecutionId).concat(".csv");
        LOG.info("report common download start...  path: {}/{}", awsConfig.getS3Bucket(), resultS3Key);
        boolean downloadResult = s3Executor.downloadObj(awsConfig.getS3Bucket(), resultS3Key, dataDirectory);
        LOG.info("report common download complete, path: {}/{}", awsConfig.getS3Bucket(), resultS3Key);
        if (downloadResult) {
            // LOAD TO MYSQL
            String clearSql = "DELETE FROM stat_lr WHERE day='" + year + month + day + "'" + " AND hour=" + executeDateTime.getHour();
            LOG.info("report common clear mysql start... sql: {}", clearSql);
            jdbcTemplate.execute(clearSql);
            LOG.info("report common clear mysql, complete");

            String sql = "LOAD DATA LOCAL INFILE '" + dataDirectory + "/" + resultS3Key + "' INTO TABLE stat_lr FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES " +
                    " (day,hour,country,platform,publisher_id,pub_app_id,placement_id,instance_id,adn_id,abt,waterfall_request,waterfall_filled,instance_request,instance_filled,video_start,video_complete,called_show,is_ready_true,is_ready_false,click,impr)";
            LOG.info("report common load 2 mysql start... sql: {}", sql);
            jdbcTemplate.execute(sql);
            LOG.info("report common load 2 mysql complete");
        }
    }


    public void userReport(LocalDate executeDate) {
        String year = executeDate.format(Constants.FORMATTER_YYYY);
        String month = executeDate.format(Constants.FORMATTER_MM);
        String day = executeDate.format(Constants.FORMATTER_DD);

        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        valueMap.put("tableName", AthenaConstants.TABLE_NAME_LR);

        String reportSql = this.templateEngine.process("report_user", new Context(null, valueMap));
        LOG.info("report user, sql:\n {}", reportSql);

        String resultS3Directory = String.join("/", "s3:/", awsConfig.getS3Bucket(), AthenaConstants.DATA_PATH_ATHENA, year, month, day);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, reportSql, resultS3Directory);
        LOG.info("report user query start...  id: {}", queryExecutionId);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("report user query complete, id: {}, result directory: {} ", queryExecutionId, resultS3Directory);

        String resultS3Key = String.join("/", AthenaConstants.DATA_PATH_ATHENA, year, month, day, queryExecutionId).concat(".csv");
        LOG.info("report user download start...  path: {}/{}", awsConfig.getS3Bucket(), resultS3Key);
        boolean downloadResult = s3Executor.downloadObj(awsConfig.getS3Bucket(), resultS3Key, dataDirectory);
        LOG.info("report user download complete, path: {}/{}", awsConfig.getS3Bucket(), resultS3Key);
        if (downloadResult) {
            // LOAD TO MYSQL
            String clearSql = "DELETE FROM stat_dau WHERE day='" + year + month + day + "'";
            LOG.info("report user clear mysql start... sql: {}", clearSql);
            jdbcTemplate.execute(clearSql);
            LOG.info("report user clear mysql, complete");

            String sql = "LOAD DATA LOCAL INFILE '" + dataDirectory + "/" + resultS3Key +
                    "' INTO TABLE stat_dau FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES " +
                    " (day,publisher_id,pub_app_id,platform,country,ip_count,did_count,dau,deu)";
            LOG.info("report user load 2 mysql start... sql: {}", sql);
            jdbcTemplate.execute(sql);
            LOG.info("report user load 2 mysql complete");
        }
    }

    public void clearDataDirectory(LocalDate executeDate) {
        String date = executeDate.format(Constants.FORMATTER_SPLIT_SLASH_YYYY_MM_DD);
        Path path = Paths.get(dataDirectory, AthenaConstants.DATA_PATH_ATHENA, date);
        try {
            LOG.info("clear data directory start... : {}", path);
            Files.deleteIfExists(path);
            LOG.info("clear data directory complete : {}", path);
        } catch (IOException e) {
            LOG.error("clear data directory, directory: {}", path);
        }

    }
}
