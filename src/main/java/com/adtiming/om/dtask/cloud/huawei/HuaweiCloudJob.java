// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud.huawei;

import com.adtiming.om.dtask.cloud.CloudConstants;
import com.adtiming.om.dtask.cloud.CloudJob;
import com.adtiming.om.dtask.cloud.huawei.HuaweiCloudClient.RdsConfig;
import com.adtiming.om.dtask.dto.DauDimensionsDTO;
import com.adtiming.om.dtask.service.AppConfig;
import com.adtiming.om.dtask.service.DictManager;
import com.adtiming.om.dtask.util.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.huawei.dli.sdk.DLIClient;
import com.huawei.dli.sdk.Database;
import com.huawei.dli.sdk.exception.DLIException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HuaweiCloudJob implements CloudJob {

    private static final Logger LOG = LogManager.getLogger();

    private final String bucketName;
    private final DLIClient dliClient;
    private final RdsConfig rdsConfig;

    @Value("${huawei.dli.database}")
    private String huaweiDliDatabase;

    @Value("${huawei.dli.data-dir}")
    private String huaweiDliDataDir;

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
    private HuaweiObsExecutor huaweiObsExecutor;

    @Resource
    private HuaweiDliExecutor huaweiDliExecutor;

    public HuaweiCloudJob(HuaweiCloudClient client) {
        this.bucketName = client.getBucket();
        this.dliClient = client.getDliClient();
        this.rdsConfig = client.getRdsConfig();
    }

    @PostConstruct
    private void init() throws DLIException {
        if (cfg.isDev()) {
            return;
        }
        initDatabase();
        initTable();
    }

    private void initDatabase() throws DLIException {
        LOG.info("init database start...");
        List<Database> databases = dliClient.listAllDatabases();
        List<String> databaseNames = databases.stream().map(Database::getDatabaseName).collect(Collectors.toList());
        if (databaseNames.contains(huaweiDliDatabase)) {
            LOG.debug("init database ignored: '{}' already exists. ", huaweiDliDatabase);
        } else {
            dliClient.createDatabase(huaweiDliDatabase);
        }
        LOG.info("init database complete");
    }

    private void initTable() {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("dliBucket", bucketName);
        valueMap.put("tableDataPath", CloudConstants.DATA_PATH_TABLE);
        valueMap.put("rdsJdbcUrl", rdsConfig.getRdsJdbcUrl());
        valueMap.put("rdsDatabase", rdsConfig.getRdsDatabase());
        valueMap.put("rdsPasswdAuth", rdsConfig.getRdsPasswdAuth());
        LOG.info("init table start...");
        for (String tableName : HuaweiConstants.TABLE_NAMES) {
            LOG.info("init table, name: {}", tableName);
            String initSql = this.templateEngine.process("cloud/huawei/init_table_" + tableName, new Context(null, valueMap));
            LOG.debug("init table, sql:\n{}", initSql);
            String jobId = huaweiDliExecutor.submitDliQuery(huaweiDliDatabase, initSql);
            LOG.info("init table complete, name: {}, job id: {}", tableName, jobId);
        }
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
        valueMap.put("dliBucket", bucketName);
        valueMap.put("tableDataPath", CloudConstants.DATA_PATH_TABLE);
        valueMap.put("tableName", tableName);

        LOG.info("add partition start..., table: {}, year: {}, month: {}, day: {}, hour: {}", tableName, year, month, day, hour);
        String dir = String.join("/", CloudConstants.DATA_PATH_TABLE, tableName, year, month, day, hour) + "/";
        huaweiObsExecutor.createDirectory(bucketName, dir);
        String partitionSql = this.templateEngine.process("cloud/huawei/add_hourly_partition", new Context(null, valueMap));
        LOG.debug("add partition sql:\n{}", partitionSql);
        String jobId = huaweiDliExecutor.submitDliQuery(huaweiDliDatabase, partitionSql);
        LOG.info("add partition complete, table: {}, year: {}, month: {}, day: {}, hour: {}, job id: {}", tableName, year, month, day, hour, jobId);
    }

    @Override
    public void commonReport(LocalDateTime executeDateTime) {
        handleHourlyReport(executeDateTime, "common", "stat_lr");
    }

    @Override
    public void cpReport(LocalDateTime executeDateTime) {
        handleHourlyReport(executeDateTime, "cp", "stat_cp");
    }

    @Override
    public void pubAppCountryReport(LocalDate executeDate) {
        handleDailyReport(executeDate, "pubapp_country", "stat_pub_app_country_uar");
    }

    @Override
    public void userReport(LocalDate executeDate) {
        handleDailyReport(executeDate, "user", "stat_dau");

        String dauDimensionsConf = dictManager.val("/om/dau_dimensions");
        if (StringUtils.isEmpty(dauDimensionsConf)) {
            LOG.info("there has no dau dimensions conf!");
            return;
        }
        DauDimensionsDTO dauDimensionsDTO;
        try {
            dauDimensionsDTO = objectMapper.readValue(dauDimensionsConf, DauDimensionsDTO.class);
        } catch (IOException e) {
            LOG.error("parse DauDimensionsDTO error:", e);
            return;
        }
        if (dauDimensionsDTO == null) {
            return;
        }
        if (dauDimensionsDTO.getAdn() != null && dauDimensionsDTO.getAdn() == 1) {
            handleDailyReport(executeDate, "user_adn", "stat_dau_adn");
        }
        if (dauDimensionsDTO.getAdn_placement() != null && dauDimensionsDTO.getAdn_placement() == 1) {
            handleDailyReport(executeDate, "user_adn_placement", "stat_dau_adn_placement");
        }
        if (dauDimensionsDTO.getPlacement() != null && dauDimensionsDTO.getPlacement() == 1) {
            handleDailyReport(executeDate, "user_placement", "stat_dau_placement");
        }
        if (dauDimensionsDTO.getInstance() != null && dauDimensionsDTO.getInstance() == 1) {
            handleDailyReport(executeDate, "user_instance", "stat_dau_instance");
        }
    }

    private void handleHourlyReport(LocalDateTime executeDateTime, String reportName, String mysqlTableName) {
        handleReport(true, executeDateTime, reportName, mysqlTableName);
    }

    private void handleDailyReport(LocalDate executeDate, String reportName, String mysqlTableName) {
        handleReport(false, LocalDateTime.of(executeDate, LocalTime.of(0, 0)), reportName, mysqlTableName);
    }

    private void handleReport(boolean isHourly, LocalDateTime executeDateTime, String reportName, String mysqlTableName) {
        String year = executeDateTime.format(Constants.FORMATTER_YYYY);
        String month = executeDateTime.format(Constants.FORMATTER_MM);
        String day = executeDateTime.format(Constants.FORMATTER_DD);
        String hour = executeDateTime.format(Constants.FORMATTER_HH);
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        valueMap.put("hour", hour);

        LOG.info("report {}, clear rds start...", reportName);
        String clearSql = "DELETE FROM " + mysqlTableName + " WHERE day='" + year + month + day + "'";
        if (isHourly) {
            clearSql += " AND hour=" + executeDateTime.getHour();
        }
        LOG.debug("report {}, clear rds sql:\n{}", reportName, clearSql);
        jdbcTemplate.execute(clearSql);
        LOG.info("report {}, clear rds, complete", reportName);

        LOG.info("report {}, query start...", reportName);
        String reportSql = this.templateEngine.process("cloud/huawei/report_" + reportName, new Context(null, valueMap));
        LOG.debug("report {}, query sql:\n{}", reportName, reportSql);
        String jobId = huaweiDliExecutor.submitDliQuery(huaweiDliDatabase, reportSql);
        LOG.info("report {}, query complete, job id: {} ", reportName, jobId);
    }

    @Override
    public void userAdRevenue(LocalDate executeDate) {
        String year = executeDate.format(Constants.FORMATTER_YYYY);
        String month = executeDate.format(Constants.FORMATTER_MM);
        String day = executeDate.format(Constants.FORMATTER_DD);
        calculateUar(year, month, day);
        splitPubAppUar(year, month, day);
        compressPubAppUar(year, month, day);
        uploadPubAppUar(year, month, day);
    }

    private void calculateUar(String year, String month, String day) {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        valueMap.put("dliBucket", bucketName);
        valueMap.put("tableDataPath", CloudConstants.DATA_PATH_TABLE);
        LOG.info("uar, query start...");
        String uarSql = this.templateEngine.process("cloud/huawei/report_uar", new Context(null, valueMap));
        LOG.debug("uar, query sql:\n{}", uarSql);
        String jobId = huaweiDliExecutor.submitDliQuery(huaweiDliDatabase, uarSql);
        LOG.info("uar, query complete, job id: {}", jobId);
    }

    private void splitPubAppUar(String year, String month, String day) {
        LOG.info("uar, download start...");
        String objectKey = String.join("/", CloudConstants.DATA_PATH_TABLE, HuaweiConstants.TABLE_NAME_REPORT_UAR, year, month, day);
        Path localPath = huaweiObsExecutor.downloadDliQueryResultObj(bucketName, objectKey, huaweiDliDataDir, year, month, day);
        LOG.info("uar, download complete");
        LOG.info("uar, split publisher app data start..., local path: {}", localPath);
        HashMap<String, BufferedWriter> pubAppUarWriters = new HashMap<>();
        try {
            Files.list(localPath).forEach(path -> {
                try (BufferedReader bufferedReader = Files.newBufferedReader(path)) {
                    String value;
                    while ((value = bufferedReader.readLine()) != null) {
                        value = value.replace("\"", "");
                        List<String> values = Splitter.on(",").limit(2).splitToList(value);
                        String pubAppId = values.get(0);
                        BufferedWriter pubAppUarWriter = pubAppUarWriters.get(pubAppId);
                        if (pubAppUarWriter == null) {
                            Path pubAappUarPath = Paths.get(huaweiDliDataDir, year + month + day, "uar", pubAppId + ".csv");
                            Files.createDirectories(pubAappUarPath.getParent());
                            pubAppUarWriter = Files.newBufferedWriter(pubAappUarPath);
                            pubAppUarWriters.put(pubAppId, pubAppUarWriter);
                        }
                        String data = values.get(1) + "\n";
                        pubAppUarWriter.write(data);
                    }
                } catch (IOException e) {
                    LOG.error("uar, split publisher app data error", e);
                }
            });
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
        Path pubAppUarLocalDir = Paths.get(huaweiDliDataDir, year + month + day, "uar");
        Path pubAppUarGzLocalDir = Paths.get(huaweiDliDataDir, year + month + day, "uar_zip");
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
        Path pubAppUarLocalDir = Paths.get(huaweiDliDataDir, year + month + day, "uar_zip");
        int pubAppUarLocalFileSize = 0;
        File[] pubAppUarLocalFiles = new File(pubAppUarLocalDir.toUri()).listFiles(pathName -> !pathName.isHidden());
        if (pubAppUarLocalFiles != null) {
            for (File pubAppUarLocalFile : pubAppUarLocalFiles) {
                String pubAppUarDataS3Key = String.join("/", CloudConstants.DATA_PATH_TABLE, "uar_data", year, month, day, pubAppUarLocalFile.getName());
                huaweiObsExecutor.uploadObject(bucketName, pubAppUarDataS3Key, pubAppUarLocalFile);
            }
            pubAppUarLocalFileSize = pubAppUarLocalFiles.length;
        }
        LOG.info("uar, upload publisher app data complete, app size: {}, local path: {}", pubAppUarLocalFileSize, pubAppUarLocalDir);
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
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", executeDate.format(Constants.FORMATTER_YYYY));
        valueMap.put("month", executeDate.format(Constants.FORMATTER_MM));
        valueMap.put("day", executeDate.format(Constants.FORMATTER_DD));

        LOG.info("collect dws publisher user, query start...");
        String reportSql = this.templateEngine.process("cloud/huawei/collect_dws_publisher_user", new Context(null, valueMap));
        LOG.debug("collect dws publisher user, query sql:\n{}", reportSql);
        String jobId = huaweiDliExecutor.submitDliQuery(huaweiDliDatabase, reportSql);
        LOG.info("collect dws publisher user, query complete, job id: {}", jobId);
    }

    @Override
    public void ltvReport(LocalDate executeDate) {
        String year = executeDate.format(Constants.FORMATTER_YYYY);
        String month = executeDate.format(Constants.FORMATTER_MM);
        String day = executeDate.format(Constants.FORMATTER_DD);

        LOG.info("report ltv, clear rds start...");
        String clearSql = "DELETE FROM stat_user_ltv WHERE day='" + year + month + day + "'";
        LOG.debug("report ltv, clear rds sql:\n{}", clearSql);
        jdbcTemplate.execute(clearSql);
        LOG.info("report lvt, clear rds, complete");

        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("executeYear", year);
        valueMap.put("executeMonth", month);
        valueMap.put("executeDay", day);

        int ltvDateRange = dictManager.intVal("/om/ltv_date_range");
        LocalDate beginDate = executeDate.minusDays(ltvDateRange);
        valueMap.put("beginYear", beginDate.format(Constants.FORMATTER_YYYY));
        valueMap.put("beginMonth", beginDate.format(Constants.FORMATTER_MM));
        valueMap.put("beginDay", beginDate.format(Constants.FORMATTER_DD));

        LOG.info("report ltv, query start...");
        String reportSql = this.templateEngine.process("cloud/huawei/report_ltv", new Context(null, valueMap));
        LOG.debug("report ltv, query sql:\n{}", reportSql);
        String jobId = huaweiDliExecutor.submitDliQuery(huaweiDliDatabase, reportSql);
        LOG.info("report ltv, query complete, job id: {}", jobId);
    }
}
