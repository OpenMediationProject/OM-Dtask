// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud.aws;

import com.adtiming.om.dtask.cloud.CloudConstants;
import com.adtiming.om.dtask.cloud.CloudJob;
import com.adtiming.om.dtask.dto.DauDimensionsDTO;
import com.adtiming.om.dtask.service.AppConfig;
import com.adtiming.om.dtask.service.DictManager;
import com.adtiming.om.dtask.service.StmtCreator;
import com.adtiming.om.dtask.util.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AwsCloudJob implements CloudJob {

    private static final Logger LOG = LogManager.getLogger();

    private final String bucketName;

    @Value("${aws.athena.database}")
    private String athenaDatabase;

    @Value("${aws.athena.data-dir}")
    private String awsAtenaDataDir;

    @Resource
    private AppConfig cfg;

    @Resource
    private DictManager dictManager;

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private TemplateEngine templateEngine;

    @Resource
    private AwsS3Executor s3Executor;

    @Resource
    private AwsAthenaExecutor athenaExecutor;


    public AwsCloudJob(AwsCloudClient client) {
        this.bucketName = client.getBucket();
    }

    @PostConstruct
    private void init() {
        if (cfg.isDev()) {
            return;
        }
        LocalDateTime executeDateTime = LocalDateTime.now(ZoneOffset.UTC);
        String year = executeDateTime.format(Constants.FORMATTER_YYYY);
        String month = executeDateTime.format(Constants.FORMATTER_MM);
        String day = executeDateTime.format(Constants.FORMATTER_DD);
        String resultDirOnS3 = getAthenaQueryResultDirOnS3(year, month, day);
        initDatabase(resultDirOnS3);
        initTable(resultDirOnS3);
    }

    private void initDatabase(String resultDirOnS3) {
        LOG.info("init database start...");
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("database", athenaDatabase);
        String initSql = this.templateEngine.process("cloud/aws/init_database", new Context(null, valueMap));
        LOG.debug("init database, sql:\n{}", initSql);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, initSql, resultDirOnS3);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("init database complete, id: {}, result path: {} ", queryExecutionId, resultDirOnS3);
    }

    private void initTable(String resultDirOnS3) {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("s3Bucket", bucketName);
        valueMap.put("tableDataPath", CloudConstants.DATA_PATH_TABLE);
        LOG.info("init table start...");
        CloudConstants.TABLE_NAMES.forEach(tableName -> {
            LOG.info("init table, name: {}", tableName);
            String initSql = this.templateEngine.process("cloud/aws/init_table_" + tableName, new Context(null, valueMap));
            LOG.debug("init table, sql:\n{}", initSql);
            String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, initSql, resultDirOnS3);
            athenaExecutor.waitForQueryToComplete(queryExecutionId);
            LOG.info("init table complete, name: {}, id: {}, result path: {} ", tableName, queryExecutionId, resultDirOnS3);
        });
        LOG.info("init table complete, all");
    }

    @Override
    public void collectDatas(LocalDateTime executeDateTime) {
        collectDatas(CloudConstants.TABLE_NAME_LR, executeDateTime);
        collectDatas(CloudConstants.TABLE_NAME_IAP, executeDateTime);
        collectDatas(CloudConstants.TABLE_NAME_CPTK, executeDateTime);
        collectDatas(CloudConstants.TABLE_NAME_EVENT, executeDateTime);
    }

    @Override
    public void collectDatas(String tableName, LocalDateTime executeDateTime) {
        String year = executeDateTime.format(Constants.FORMATTER_YYYY);
        String month = executeDateTime.format(Constants.FORMATTER_MM);
        String day = executeDateTime.format(Constants.FORMATTER_DD);
        String hour = executeDateTime.format(Constants.FORMATTER_HH);
        addPartition(tableName, year, month, day, hour);
    }

    private void addPartition(String tableName, String year, String month, String day, String hour) {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        valueMap.put("hour", hour);
        valueMap.put("s3Bucket", bucketName);
        valueMap.put("tableDataPath", CloudConstants.DATA_PATH_TABLE);
        valueMap.put("tableName", tableName);
        addPartition("add_hourly_partition", valueMap);
    }

    @Override
    public void commonReport(LocalDateTime executeDateTime) {
        String mysqlTableColumns = "day,hour,country,platform,publisher_id,pub_app_id,placement_id,instance_id,scene_id,adn_id,abt,abt_id,bid,rule_id,app_version,sdk_version,os_version,waterfall_request,waterfall_filled,instance_request,instance_filled,video_start,video_complete,called_show,is_ready_true,is_ready_false,click,impr,bid_req,bid_resp,bid_resp_price,bid_win,bid_win_price ";
        handleHourlyReport(executeDateTime, "common", "stat_lr", mysqlTableColumns);
    }

    @Override
    public void cpReport(LocalDateTime executeDateTime) {
        String mysqlTableColumns = "day,hour,publisher_id,pub_app_id,placement_id,country,app_id,campaign_id,creative_id,impr,click,win_price";
        handleHourlyReport(executeDateTime, "cp", "stat_cp", mysqlTableColumns);
    }

    @Override
    public void pubAppCountryReport(LocalDate executeDate) {
        String mysqlTableColumns = "pub_app_id,country,day,uar1,uar2,uar3,uar4,uar5";
        handleDailyReport(executeDate, "pubapp_country", "stat_pub_app_country_uar", mysqlTableColumns);
    }

    @Override
    public void userReport(LocalDate executeDate) {
        handleDailyReport(executeDate, "user", "stat_dau", "day,publisher_id,pub_app_id,platform,country,app_version,sdk_version,os_version,ip_count,did_count,dau,deu ");

        handleDailyReport(executeDate, "user_abtest", "stat_dau_abtest", "day,publisher_id,pub_app_id,platform,abt,abt_id,country,app_version,sdk_version,os_version,ip_count,did_count,dau,deu ");

        String dauDimensionsConf = dictManager.val("/om/dau_dimensions");
        if (StringUtils.isEmpty(dauDimensionsConf)) {
            LOG.error("There has no dau dimensions conf!");
            return;
        }
        DauDimensionsDTO dauDimensionsDTO = null;
        try {
            dauDimensionsDTO = objectMapper.readValue(dauDimensionsConf, DauDimensionsDTO.class);
        } catch (IOException e) {
            LOG.error("Parse DauDimensionsDTO error:", e);
            return;
        }
        if (dauDimensionsDTO == null) {
            return;
        }
        if (dauDimensionsDTO.getAdn() != null && dauDimensionsDTO.getAdn() == 1) {
            handleDailyReport(executeDate, "user_adn", "stat_dau_adn", "day,publisher_id,pub_app_id,platform,country,adn_id,app_version,sdk_version,os_version,ip_count,did_count,dau,deu");
        }
        if (dauDimensionsDTO.getAdn_placement() != null && dauDimensionsDTO.getAdn_placement() == 1) {
            handleDailyReport(executeDate, "user_adn_placement", "stat_dau_adn_placement", "day,publisher_id,pub_app_id,platform,country,placement_id,adn_id,app_version,sdk_version,os_version,ip_count,did_count,dau,deu");
        }
        if (dauDimensionsDTO.getPlacement() != null && dauDimensionsDTO.getPlacement() == 1) {
            handleDailyReport(executeDate, "user_placement", "stat_dau_placement", "day,publisher_id,pub_app_id,platform,country,placement_id,app_version,sdk_version,os_version,ip_count,did_count,dau,deu");
        }
        if (dauDimensionsDTO.getInstance() != null && dauDimensionsDTO.getInstance() == 1) {
            handleDailyReport(executeDate, "user_instance", "stat_dau_instance", "day,publisher_id,pub_app_id,platform,country,placement_id,instance_id,adn_id,app_version,sdk_version,os_version,ip_count,did_count,dau,deu");
        }
    }

    private void handleHourlyReport(LocalDateTime executeDateTime, String reportName, String mysqlTableName, String mysqlTableColumns) {
        handleReport(true, executeDateTime, reportName, mysqlTableName, mysqlTableColumns);
    }

    private void handleDailyReport(LocalDate executeDate, String reportName, String mysqlTableName, String mysqlTableColumns) {
        handleReport(false, LocalDateTime.of(executeDate, LocalTime.of(0, 0)), reportName, mysqlTableName, mysqlTableColumns);
    }

    private void handleReport(boolean isHourly, LocalDateTime executeDateTime, String reportName, String mysqlTableName, String mysqlTableColumns) {
        try {
            String year = executeDateTime.format(Constants.FORMATTER_YYYY);
            String month = executeDateTime.format(Constants.FORMATTER_MM);
            String day = executeDateTime.format(Constants.FORMATTER_DD);
            String hour = executeDateTime.format(Constants.FORMATTER_HH);
            HashMap<String, Object> valueMap = new HashMap<>();
            valueMap.put("year", year);
            valueMap.put("month", month);
            valueMap.put("day", day);
            valueMap.put("hour", hour);

            LOG.info("report {}, query start...", reportName);
            String reportSql = this.templateEngine.process("cloud/aws/report_" + reportName, new Context(null, valueMap));
            LOG.debug("report {}, query sql:\n{}", reportName, reportSql);
            String resultDirOnS3 = getAthenaQueryResultDirOnS3(year, month, day);
            String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, reportSql, resultDirOnS3);
            athenaExecutor.waitForQueryToComplete(queryExecutionId);
            LOG.info("report {}, query complete, id: {}, result directory: {} ", reportName, queryExecutionId, resultDirOnS3);

            LOG.info("report {}, download start...", reportName);
            Path localPath = s3Executor.downloadAthenaQueryResultObj(awsAtenaDataDir, bucketName, year, month, day, queryExecutionId);
            LOG.info("report {}, download complete", reportName);

            // LOAD TO MYSQL
            LOG.info("report {}, clear mysql start...", reportName);
            String clearSql = "DELETE FROM " + mysqlTableName + " WHERE day='" + year + month + day + "'";
            if (isHourly) {
                clearSql += " AND hour=" + executeDateTime.getHour();
            }
            LOG.debug("report {}, clear mysql sql:\n{}", reportName, clearSql);
            jdbcTemplate.execute(clearSql);
            LOG.info("report {}, clear mysql, complete", reportName);

            LOG.info("report {}, load 2 mysql start...", reportName);
            String sql = "LOAD DATA LOCAL INFILE '" + localPath + "' INTO TABLE " + mysqlTableName + " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES " + " (" + mysqlTableColumns + ")";
            LOG.debug("report {}, load 2 mysql sql:\n{}", reportName, sql);
            jdbcTemplate.execute(sql);
            LOG.info("report {}, load 2 mysql complete", reportName);
        } catch (Exception e) {
            LOG.error("Do report failed! Name {} mysql table name {}", reportName, mysqlTableName, e);
        }
    }

    public void syncOdsOmAdnetwork2Athena(LocalDate executeDate) {
        String year = executeDate.format(Constants.FORMATTER_YYYY);
        String month = executeDate.format(Constants.FORMATTER_MM);
        String day = executeDate.format(Constants.FORMATTER_DD);
        Path odsOmAdnetworkPath = dumpOdsOmAdnetworkData(year, month, day);
        String s3Key = String.join("/", CloudConstants.DATA_PATH_TABLE, CloudConstants.TABLE_NAME_ODS_OM_ADNETWORK, "data.dsv");
        LOG.info("upload om adnetwork, start...");
        s3Executor.uploadObject(bucketName, s3Key, new File(odsOmAdnetworkPath.toUri()));
        LOG.info("upload om adnetwork, complete");
    }

    private Path dumpOdsOmAdnetworkData(String year, String month, String day) {
        LOG.info("dump om adnetwork, start...");
        String odsOmAdnetworkFile = String.join("/", year + month + day, "mysql", CloudConstants.TABLE_NAME_ODS_OM_ADNETWORK + ".dsv");
        Path odsOmAdnetworkPath = Paths.get(awsAtenaDataDir, odsOmAdnetworkFile);
        try {
            Files.createDirectories(odsOmAdnetworkPath.getParent());
        } catch (IOException e) {
            throw new RuntimeException("dump om adnetwork, create local dir error", e);
        }
        String sql = "select id, name, class_name, descn from om_adnetwork";
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(odsOmAdnetworkPath)) {
            jdbcTemplate.query(new StmtCreator(sql), rs -> {
                String data = String.join("\u0001",
                        rs.getString("id"),
                        rs.getString("name"),
                        rs.getString("class_name"),
                        rs.getString("descn"),
                        "\n"
                );
                try {
                    bufferedWriter.write(data);
                } catch (IOException e) {
                    LOG.error("dump om adnetwork, write data error: {}", data, e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("uar, dump om adnetwork, write data error", e);
        }
        LOG.info("dump om adnetwork, complete");
        return odsOmAdnetworkPath;
    }

    public void syncOdsStatAdnetwork2Athena(LocalDate executeDate) {
        String year = executeDate.format(Constants.FORMATTER_YYYY);
        String month = executeDate.format(Constants.FORMATTER_MM);
        String day = executeDate.format(Constants.FORMATTER_DD);

        Path odsStatAdnetworkPath = dumpOdsStatAdnetworkData(year, month, day);
        String s3Key = String.join("/", CloudConstants.DATA_PATH_TABLE, CloudConstants.TABLE_NAME_ODS_STAT_ADNETWORK, year, month, day, "data.dsv");
        LOG.info("upload stat adnetwork, start...");
        s3Executor.uploadObject(bucketName, s3Key, new File(odsStatAdnetworkPath.toUri()));
        LOG.info("upload stat adnetwork, complete");
        addOdsStatAdnetworkPartition(year, month, day);
    }

    private Path dumpOdsStatAdnetworkData(String year, String month, String day) {
        LOG.info("dump stat adnetwork, start...");
        String odsStatAdnetworkFile = String.join("/", year + month + day, "mysql", CloudConstants.TABLE_NAME_ODS_STAT_ADNETWORK + ".dsv");
        Path odsStatAdnetworkPath = Paths.get(awsAtenaDataDir, odsStatAdnetworkFile);
        try {
            Files.createDirectories(odsStatAdnetworkPath.getParent());
        } catch (IOException e) {
            LOG.error("dump stat adnetwork, create local dir error", e);
            throw new RuntimeException("uar, dump stat adnetwork, create local dir error", e);
        }
        String sql = "select id, publisher_id, pub_app_id, platform, adn_id, country, instance_id, cost from stat_adnetwork where day=?";
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(odsStatAdnetworkPath)) {
            jdbcTemplate.query(new StmtCreator(sql, String.join("-", year, month, day)), rs -> {
                String data = String.join("\u0001",
                        rs.getString("id"),
                        rs.getString("publisher_id"),
                        rs.getString("pub_app_id"),
                        rs.getString("platform"),
                        rs.getString("adn_id"),
                        rs.getString("country"),
                        rs.getString("instance_id"),
                        rs.getString("cost"),
                        "\n"
                );
                try {
                    bufferedWriter.write(data);
                } catch (IOException e) {
                    LOG.error("dump stat adnetwork, write data error", e);
                    throw new RuntimeException("uar, dump stat adnetwork, write data error", e);
                }
            });
        } catch (IOException e) {
            LOG.error("dump stat adnetwork, write data error", e);
            throw new RuntimeException("uar, dump stat adnetwork, write data error", e);
        }
        LOG.info("dump stat adnetwork, complete");
        return odsStatAdnetworkPath;
    }

    private void addOdsStatAdnetworkPartition(String year, String month, String day) {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        valueMap.put("s3Bucket", bucketName);
        valueMap.put("tableDataPath", CloudConstants.DATA_PATH_TABLE);
        valueMap.put("tableName", CloudConstants.TABLE_NAME_ODS_STAT_ADNETWORK);
        addPartition("add_daily_partition", valueMap);
    }

    @Override
    public void userAdRevenue(LocalDate executeDate) {
        String year = executeDate.format(Constants.FORMATTER_YYYY);
        String month = executeDate.format(Constants.FORMATTER_MM);
        String day = executeDate.format(Constants.FORMATTER_DD);

        String queryExecutionId = calculateUar(year, month, day);
        splitPubAppUar(year, month, day, queryExecutionId);
        compressPubAppUar(year, month, day);
        uploadPubAppUar(year, month, day);
    }

    private String calculateUar(String year, String month, String day) {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        LOG.info("uar, query start...");
        String uarSql = this.templateEngine.process("cloud/aws/user_ad_revenue", new Context(null, valueMap));
        LOG.debug("uar, query sql:\n{}", uarSql);
        String resultDirOnS3 = getAthenaQueryResultDirOnS3(year, month, day);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, uarSql, resultDirOnS3);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("uar, query complete, id: {}, result directory: {} ", queryExecutionId, resultDirOnS3);
        return queryExecutionId;
    }

    private void splitPubAppUar(String year, String month, String day, String queryExecutionId) {
        LOG.info("uar, download start...");
        Path localPath = s3Executor.downloadAthenaQueryResultObj(awsAtenaDataDir, bucketName, year, month, day, queryExecutionId);
        LOG.info("uar, download complete");
        LOG.info("uar, split publisher app data start..., local path: {}", localPath);
        HashMap<String, BufferedWriter> pubAppUarWriters = new HashMap<>();
        try (BufferedReader bufferedReader = Files.newBufferedReader(localPath)) {
            String value = bufferedReader.readLine(); // delete first line
            while ((value = bufferedReader.readLine()) != null) {
                value = value.replace("\"", "");
                List<String> values = Splitter.on(",").limit(2).splitToList(value);
                String pubAppId = values.get(0);
                BufferedWriter pubAppUarWriter = pubAppUarWriters.get(pubAppId);
                if (pubAppUarWriter == null) {
                    Path pubAappUarPath = Paths.get(awsAtenaDataDir, year + month + day, "uar", pubAppId + ".csv");
                    Files.createDirectories(pubAappUarPath.getParent());
                    pubAppUarWriter = Files.newBufferedWriter(pubAappUarPath);
                    pubAppUarWriters.put(pubAppId, pubAppUarWriter);
                }
                String data = values.get(1) + "\n";
                pubAppUarWriter.write(data);
            }
            pubAppUarWriters.forEach((k, v) -> {
                try {
                    v.close();
                } catch (IOException e) {
                    LOG.error("uar, split publisher app data error", e);
                }
            });

        } catch (IOException e) {
            LOG.error("uar, split publisher app data error", e);
        }
        LOG.info("uar, split publisher app data complete, local path: {}", localPath);
    }

    private void compressPubAppUar(String year, String month, String day) {
        LOG.info("uar, compresss publisher app data start...");
        Path pubAppUarLocalDir = Paths.get(awsAtenaDataDir, year + month + day, "uar");
        Path pubAppUarGzLocalDir = Paths.get(awsAtenaDataDir, year + month + day, "uar_zip");
        try {
            Files.createDirectories(pubAppUarGzLocalDir);
        } catch (IOException e) {
            LOG.info("uar, compresss publisher app data, create dir fail, local path: {}", pubAppUarGzLocalDir);
            throw new RuntimeException("uar, compresss publisher app data, create dir fail", e);
        }
        int pubAppUarLocalFileSize = 0;
        File[] pubAppUarLocalFiles = new File(pubAppUarLocalDir.toUri()).listFiles(pathName -> !pathName.isHidden());
        if (pubAppUarLocalFiles != null) {
            for (File pubAppUarLocalFile : pubAppUarLocalFiles) {
                compress2Zip(pubAppUarGzLocalDir, pubAppUarLocalFile);
            }
            pubAppUarLocalFileSize = pubAppUarLocalFiles.length;
        }
        LOG.info("uar, compresss publisher app data complete, app size: {}, local path: {}", pubAppUarLocalFileSize, pubAppUarGzLocalDir);
    }

    private void uploadPubAppUar(String year, String month, String day) {
        LOG.info("uar, upload publisher app data start...");
        Path pubAppUarLocalDir = Paths.get(awsAtenaDataDir, year + month + day, "uar_zip");
        int pubAppUarLocalFileSize = 0;
        File[] pubAppUarLocalFiles = new File(pubAppUarLocalDir.toUri()).listFiles(pathName -> !pathName.isHidden());
        if (pubAppUarLocalFiles != null) {
            for (File pubAppUarLocalFile : pubAppUarLocalFiles) {
                String pubAppUarDataS3Key = String.join("/", CloudConstants.DATA_PATH_TABLE, "uar_data", year, month, day, pubAppUarLocalFile.getName());
                s3Executor.uploadObject(bucketName, pubAppUarDataS3Key, pubAppUarLocalFile);
            }
            pubAppUarLocalFileSize = pubAppUarLocalFiles.length;
        }
        LOG.info("uar, upload publisher app data complete, app size: {}, local path: {}", pubAppUarLocalFileSize, pubAppUarLocalDir);
    }

    private void addPartition(String templateName, Map<String, Object> valueMap) {
        String tableName = (String) valueMap.get("tableName");
        String year = (String) valueMap.get("year");
        String month = (String) valueMap.get("month");
        String day = (String) valueMap.get("day");
        String hour = (String) valueMap.get("hour");
        LOG.info("add partition start..., table: {}, year: {}, month: {}, day: {}, hour: {}", tableName, year, month, day, hour);
        String partitionSql = this.templateEngine.process("cloud/aws/" + templateName, new Context(null, valueMap));
        LOG.debug("add partition sql:\n{}", partitionSql);
        String resultDirOnS3 = getAthenaQueryResultDirOnS3(year, month, day);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, partitionSql, resultDirOnS3);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("add partition complete, table: {}, year: {}, month: {}, day: {}, hour: {}, id: {}, result path: {} ", tableName, year, month, day, hour, queryExecutionId, resultDirOnS3);
    }

    private String getAthenaQueryResultDirOnS3(String year, String month, String day) {
        return String.join("/", "s3:/", bucketName, AwsConstants.DATA_PATH_ATHENA, year, month, day);
    }

    private void compress2Zip(Path parentZipPath, File dataFile) {
        Path zipFilePath = Paths.get(parentZipPath.toAbsolutePath().toString(), dataFile.getName() + ".zip");
        try (ZipArchiveOutputStream zipArchiveOutputStream = new ZipArchiveOutputStream(zipFilePath.toFile())) {
            ZipArchiveEntry zipArchiveEntry = new ZipArchiveEntry(dataFile, dataFile.getName());
            zipArchiveOutputStream.putArchiveEntry(zipArchiveEntry);
            try (InputStream is = new BufferedInputStream(new FileInputStream(dataFile))) {
                byte[] buffer = new byte[1024 * 5];
                int len;
                while ((len = is.read(buffer)) != -1) {
                    zipArchiveOutputStream.write(buffer, 0, len);
                }
                zipArchiveOutputStream.closeArchiveEntry();
            } catch (Exception e) {
                LOG.error("compress 2 zip fail ", e);
                throw new RuntimeException("compress 2 zip fail", e);
            }
            zipArchiveOutputStream.finish();
        } catch (IOException e) {
            LOG.error("compress 2 zip fail ", e);
            throw new RuntimeException("compress 2 zip fail", e);
        }
    }

    @Override
    public void collectDwsPublisherUser(LocalDate executeDate) {
        Object collectDate = executeDate.format(Constants.FORMATTER_YYYYMMDD);
        String s3Key = String.join("/", CloudConstants.DATA_PATH_TABLE, CloudConstants.TABLE_NAME_DWS_PUBLISHER_USER, "ymd=" + collectDate + "/");
        LOG.info("collect dws publisher user, clean dir, start...");
        s3Executor.deleteObject(bucketName, s3Key);
        LOG.info("collect dws publisher user, clean dir, complete");

        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", executeDate.format(Constants.FORMATTER_YYYY));
        valueMap.put("month", executeDate.format(Constants.FORMATTER_MM));
        valueMap.put("day", executeDate.format(Constants.FORMATTER_DD));

        LOG.info("collect dws publisher user, query start...");
        String reportSql = this.templateEngine.process("cloud/aws/collect_dws_publisher_user", new Context(null, valueMap));
        LOG.debug("collect dws publisher user, query sql:\n{}", reportSql);
        String resultDirOnS3 = getAthenaQueryResultDirOnS3(executeDate.format(Constants.FORMATTER_YYYY), executeDate.format(Constants.FORMATTER_MM), executeDate.format(Constants.FORMATTER_DD));
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, reportSql, resultDirOnS3);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("collect dws publisher user,, query complete, id: {}, result directory: {} ", queryExecutionId, resultDirOnS3);
    }

    @Override
    public void ltvReport(LocalDate executeDate) {
        String year = executeDate.format(Constants.FORMATTER_YYYY);
        String month = executeDate.format(Constants.FORMATTER_MM);
        String day = executeDate.format(Constants.FORMATTER_DD);

        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("executeYear", year);
        valueMap.put("executeMonth", month);
        valueMap.put("executeDay", day);

        int ltvDateRange = dictManager.intVal("/om/ltv_date_range");
        LocalDate beginDate = executeDate.minusDays(ltvDateRange);
        valueMap.put("beginYear", beginDate.format(Constants.FORMATTER_YYYY));
        valueMap.put("beginMonth", beginDate.format(Constants.FORMATTER_MM));
        valueMap.put("beginDay", beginDate.format(Constants.FORMATTER_DD));

        LOG.info("ltv report, query start...");
        String reportSql = this.templateEngine.process("cloud/aws/report_ltv", new Context(null, valueMap));
        LOG.debug("ltv report, query sql:\n{}", reportSql);
        String resultDirOnS3 = getAthenaQueryResultDirOnS3(year, month, day);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, reportSql, resultDirOnS3);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("ltv report, query complete, id: {}, result directory: {} ", queryExecutionId, resultDirOnS3);

        LOG.info("ltv report, download start...");
        Path localPath = s3Executor.downloadAthenaQueryResultObj(awsAtenaDataDir, bucketName, year, month, day, queryExecutionId);
        LOG.info("ltv report, download complete");

        // LOAD TO MYSQL
        LOG.info("ltv report, clear, clear mysql start...");
        String clearSql = "DELETE FROM stat_user_ltv WHERE day='" + year + month + day + "'";
        LOG.debug("ltv report, clear mysql sql:\n{}", clearSql);
        jdbcTemplate.execute(clearSql);
        LOG.info("ltv report, clear mysql, complete");

        LOG.info("ltv report, load 2 mysql start...");
        String sql = "LOAD DATA LOCAL INFILE '" + localPath +
                "' INTO TABLE stat_user_ltv FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES " +
                " (day,base_date,retention_day,country,publisher_id,pub_app_id,user_cnt_new,user_cnt_old,retention_cnt_new,retention_cnt_old,mediation_value_new,mediation_value_old,iap_value_new,iap_value_old,total_value_new,total_value_old)";
        LOG.debug("ltv report, load 2 mysql sql:\n{}", sql);
        jdbcTemplate.execute(sql);
        LOG.info("ltv report, load 2 mysql complete");
    }
}
