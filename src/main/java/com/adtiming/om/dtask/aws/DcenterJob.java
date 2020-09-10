// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.aws;

import com.adtiming.om.dtask.service.StmtCreator;
import com.adtiming.om.dtask.util.Constants;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Splitter;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
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
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;

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
        LocalDateTime executeDateTime = LocalDateTime.now(ZoneOffset.UTC);
        String year = executeDateTime.format(Constants.FORMATTER_YYYY);
        String month = executeDateTime.format(Constants.FORMATTER_MM);
        String day = executeDateTime.format(Constants.FORMATTER_DD);
        String resultDirOnS3 = getAthenaQueryResultDirOnS3(year, month, day);
        new Thread(() -> {
            initDatabase(resultDirOnS3);
            initTable(resultDirOnS3);
        }, "s3Init").start();
    }

    private void initDatabase(String resultDirOnS3) {
        LOG.info("init database start...");
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("database", athenaDatabase);
        String initSql = this.templateEngine.process("init_database", new Context(null, valueMap));
        LOG.debug("init database, sql:\n {}", initSql);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, initSql, resultDirOnS3);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("init database complete, id: {}, result path: {} ", queryExecutionId, resultDirOnS3);
    }

    private void initTable(String resultDirOnS3) {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("s3Bucket", awsConfig.getS3Bucket());
        valueMap.put("tableDataPath", AthenaConstants.DATA_PATH_TABLE);
        LOG.info("init table start...");
        AthenaConstants.TABLE_NAMES.forEach(tableName -> {
            LOG.info("init table, name: {}", tableName);
            String initSql = this.templateEngine.process("init_table_" + tableName, new Context(null, valueMap));
            LOG.debug("init table, sql:\n {}", initSql);
            String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, initSql, resultDirOnS3);
            athenaExecutor.waitForQueryToComplete(queryExecutionId);
            LOG.info("init table complete, name: {}, id: {}, result path: {} ", tableName, queryExecutionId, resultDirOnS3);
        });
        LOG.info("init table complete, all");
    }

    public void commonReport(LocalDateTime executeDateTime) {
        String year = executeDateTime.format(Constants.FORMATTER_YYYY);
        String month = executeDateTime.format(Constants.FORMATTER_MM);
        String day = executeDateTime.format(Constants.FORMATTER_DD);
        String hour = executeDateTime.format(Constants.FORMATTER_HH);

        addLrPartition(year, month, day, hour);

        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        valueMap.put("hour", hour);
        valueMap.put("tableName", AthenaConstants.TABLE_NAME_LR);

        LOG.info("report common, query start...");
        String reportSql = this.templateEngine.process("report_common", new Context(null, valueMap));
        LOG.debug("report common, query sql:\n {}", reportSql);
        String resultDirOnS3 = getAthenaQueryResultDirOnS3(year, month, day);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, reportSql, resultDirOnS3);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("report common, query complete, id: {}, result directory: {} ", queryExecutionId, resultDirOnS3);

        LOG.info("report common, download start...");
        Path localPath = s3Executor.downloadAthenaQueryResultObj(dataDirectory, awsConfig.getS3Bucket(), year, month, day, queryExecutionId);
        LOG.info("report common, download complete");

        // LOAD TO MYSQL
        LOG.info("report common, clear mysql start...");
        String clearSql = "DELETE FROM stat_lr WHERE day='" + year + month + day + "'" + " AND hour=" + executeDateTime.getHour();
        LOG.debug("report common, clear mysql sql:\n {}", clearSql);
        jdbcTemplate.execute(clearSql);
        LOG.info("report common, clear mysql, complete");

        LOG.info("report common, load 2 mysql start...");
        String sql = "LOAD DATA LOCAL INFILE '" + localPath + "' INTO TABLE stat_lr FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES " +
                " (day,hour,country,platform,publisher_id,pub_app_id,placement_id,instance_id,scene_id,adn_id,abt,bid,waterfall_request,waterfall_filled,instance_request,instance_filled,video_start,video_complete,called_show,is_ready_true,is_ready_false,click,impr,bid_req,bid_resp,bid_resp_price,bid_win,bid_win_price)";
        LOG.debug("report common, load 2 mysql sql:\n {}", sql);
        jdbcTemplate.execute(sql);
        LOG.info("report common, load 2 mysql complete");
    }

    private void addLrPartition(String year, String month, String day, String hour) {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        valueMap.put("hour", hour);
        valueMap.put("s3Bucket", awsConfig.getS3Bucket());
        valueMap.put("tableDataPath", AthenaConstants.DATA_PATH_TABLE);
        valueMap.put("tableName", AthenaConstants.TABLE_NAME_LR);
        addPartition("add_hourly_partition", valueMap);
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

        LOG.info("report user, query start...");
        String reportSql = this.templateEngine.process("report_user", new Context(null, valueMap));
        LOG.debug("report user, query sql:\n {}", reportSql);
        String resultDirOnS3 = getAthenaQueryResultDirOnS3(year, month, day);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, reportSql, resultDirOnS3);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("report user, query complete, id: {}, result directory: {} ", queryExecutionId, resultDirOnS3);

        LOG.info("report user, download start...");
        Path localPath = s3Executor.downloadAthenaQueryResultObj(dataDirectory, awsConfig.getS3Bucket(), year, month, day, queryExecutionId);
        LOG.info("report user, download complete");

        // LOAD TO MYSQL
        LOG.info("report user, clear, clear mysql start...");
        String clearSql = "DELETE FROM stat_dau WHERE day='" + year + month + day + "'";
        LOG.debug("report user, clear mysql sql:\n {}", clearSql);
        jdbcTemplate.execute(clearSql);
        LOG.info("report user, clear mysql, complete");

        LOG.info("report user, load 2 mysql start...");
        String sql = "LOAD DATA LOCAL INFILE '" + localPath +
                "' INTO TABLE stat_dau FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES " +
                " (day,publisher_id,pub_app_id,platform,country,ip_count,did_count,dau,deu)";
        LOG.debug("report user, load 2 mysql sql:\n {}", sql);
        jdbcTemplate.execute(sql);
        LOG.info("report user, load 2 mysql complete");
    }

    public void clearTmpLocalDataDirectory(LocalDate executeDate) {
        Path tmpLocalPath = Paths.get(dataDirectory, executeDate.format(Constants.FORMATTER_YYYYMMDD));
        try {
            LOG.info("clear tmp local data directory start... : {}", tmpLocalPath);
            FileUtils.deleteDirectory(new File(tmpLocalPath.toUri()));
            LOG.info("clear tmp local data directory complete : {}", tmpLocalPath);
        } catch (IOException e) {
            LOG.error("clear tmp local data directory, directory: {}", tmpLocalPath, e);
        }
    }

    public void userAdRevenue(LocalDate executeDate) {
        String year = executeDate.format(Constants.FORMATTER_YYYY);
        String month = executeDate.format(Constants.FORMATTER_MM);
        String day = executeDate.format(Constants.FORMATTER_DD);

        syncOdsOmAdnetwork2Athena(year, month, day);
        syncOdsStatAdnetwork2Athena(year, month, day);
        String queryExecutionId = calculateUar(year, month, day);
        splitPubAppUar(year, month, day, queryExecutionId);
        compressPubAppUar(year, month, day);
        uploadPubAppUar(year, month, day);
    }

    private void syncOdsOmAdnetwork2Athena(String year, String month, String day) {
        Path odsOmAdnetworkPath = dumpOdsOmAdnetworkData(year, month, day);
        String s3Key = String.join("/", AthenaConstants.DATA_PATH_TABLE, AthenaConstants.TABLE_NAME_ODS_OM_ADNETWORK, "data.dsv");
        LOG.info("uar, upload om adnetwork, start...");
        s3Executor.uploadObject(awsConfig.getS3Bucket(), s3Key, new File(odsOmAdnetworkPath.toUri()));
        LOG.info("uar, upload om adnetwork, complete");
    }

    private Path dumpOdsOmAdnetworkData(String year, String month, String day) {
        LOG.info("uar, dump om adnetwork, start...");
        String odsOmAdnetworkFile = String.join("/", year + month + day, "mysql", AthenaConstants.TABLE_NAME_ODS_OM_ADNETWORK + ".dsv");
        Path odsOmAdnetworkPath = Paths.get(dataDirectory, odsOmAdnetworkFile);
        try {
            Files.createDirectories(odsOmAdnetworkPath.getParent());
        } catch (IOException e) {
            throw new RuntimeException("uar, dump om adnetwork, create local dir error", e);
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
                    LOG.error("uar, dump om adnetwork, write data error: {}", data, e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("uar, dump om adnetwork, write data error", e);
        }
        LOG.info("uar, dump om adnetwork, complete");
        return odsOmAdnetworkPath;
    }

    private void syncOdsStatAdnetwork2Athena(String year, String month, String day) {
        Path odsStatAdnetworkPath = dumpOdsStatAdnetworkData(year, month, day);
        String s3Key = String.join("/", AthenaConstants.DATA_PATH_TABLE, AthenaConstants.TABLE_NAME_ODS_STAT_ADNETWORK, year, month, day, "data.dsv");
        LOG.info("uar, upload stat adnetwork, start...");
        s3Executor.uploadObject(awsConfig.getS3Bucket(), s3Key, new File(odsStatAdnetworkPath.toUri()));
        LOG.info("uar, upload stat adnetwork, complete");
        addOdsStatAdnetworkPartition(year, month, day);
    }

    private Path dumpOdsStatAdnetworkData(String year, String month, String day) {
        LOG.info("uar, dump stat adnetwork, start...");
        String odsStatAdnetworkFile = String.join("/", year + month + day, "mysql", AthenaConstants.TABLE_NAME_ODS_STAT_ADNETWORK + ".dsv");
        Path odsStatAdnetworkPath = Paths.get(dataDirectory, odsStatAdnetworkFile);
        try {
            Files.createDirectories(odsStatAdnetworkPath.getParent());
        } catch (IOException e) {
            LOG.error("uar, dump stat adnetwork, create local dir error", e);
            throw new RuntimeException("uar, dump stat adnetwork, create local dir error", e);
        }
        String sql = "select id, pub_app_id, platform, adn_id, country, instance_id, cost from stat_adnetwork where day=?";
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(odsStatAdnetworkPath)) {
            jdbcTemplate.query(new StmtCreator(sql, String.join("-", year, month, day)), rs -> {
                String data = String.join("\u0001",
                        rs.getString("id"),
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
                    LOG.error("uar, dump stat adnetwork, write data error", e);
                    throw new RuntimeException("uar, dump stat adnetwork, write data error", e);
                }
            });
        } catch (IOException e) {
            LOG.error("uar, dump stat adnetwork, write data error", e);
            throw new RuntimeException("uar, dump stat adnetwork, write data error", e);
        }
        LOG.info("uar, dump stat adnetwork, complete");
        return odsStatAdnetworkPath;
    }

    public void addOdsStatAdnetworkPartition(String year, String month, String day) {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        valueMap.put("s3Bucket", awsConfig.getS3Bucket());
        valueMap.put("tableDataPath", AthenaConstants.DATA_PATH_TABLE);
        valueMap.put("tableName", AthenaConstants.TABLE_NAME_ODS_STAT_ADNETWORK);
        addPartition("add_daily_partition", valueMap);
    }

    private String calculateUar(String year, String month, String day) {
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("year", year);
        valueMap.put("month", month);
        valueMap.put("day", day);
        LOG.info("uar, query start...");
        String uarSql = this.templateEngine.process("user_ad_revenue", new Context(null, valueMap));
        LOG.debug("uar, query sql:\n {}", uarSql);
        String resultDirOnS3 = getAthenaQueryResultDirOnS3(year, month, day);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, uarSql, resultDirOnS3);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("uar, query complete, id: {}, result directory: {} ", queryExecutionId, resultDirOnS3);
        return queryExecutionId;
    }

    private void splitPubAppUar(String year, String month, String day, String queryExecutionId) {
        LOG.info("uar, download start...");
        Path localPath = s3Executor.downloadAthenaQueryResultObj(dataDirectory, awsConfig.getS3Bucket(), year, month, day, queryExecutionId);
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
                    Path pubAappUarPath = Paths.get(dataDirectory, year + month + day, "uar", pubAppId + ".csv");
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
        Path pubAppUarLocalDir = Paths.get(dataDirectory, year + month + day, "uar");
        Path pubAppUarGzLocalDir = Paths.get(dataDirectory, year + month + day, "uar_gz");
        try {
            Files.createDirectories(pubAppUarGzLocalDir);
        } catch (IOException e) {
            LOG.info("uar, compresss publisher app data, create dir fail, local path: {}", pubAppUarGzLocalDir);
            throw new RuntimeException("uar, compresss publisher app data, create dir fail", e);
        }
        int pubAppUarLocalFileSize = 0;
        File[] pubAppUarLocalFiles = new File(pubAppUarLocalDir.toUri()).listFiles();
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
        Path pubAppUarLocalDir = Paths.get(dataDirectory, year + month + day, "uar_gz");
        int pubAppUarLocalFileSize = 0;
        File[] pubAppUarLocalFiles = new File(pubAppUarLocalDir.toUri()).listFiles();
        if (pubAppUarLocalFiles != null) {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentType("text/csv");
            objectMetadata.setContentEncoding("gzip");
            for (File pubAppUarLocalFile : pubAppUarLocalFiles) {
                String pubAppUarDataS3Key = String.join("/", AthenaConstants.DATA_PATH_TABLE, "uar_data", year, month, day, pubAppUarLocalFile.getName());
                s3Executor.uploadObject(awsConfig.getS3Bucket(), pubAppUarDataS3Key, pubAppUarLocalFile, objectMetadata);
            }
            pubAppUarLocalFileSize = pubAppUarLocalFiles.length;
        }
        LOG.info("uar, upload publisher app data complete, app size: {}, local path: {}", pubAppUarLocalFileSize, pubAppUarLocalDir);
    }

    private void addPartition(String templateName, HashMap<String, Object> valueMap) {
        String tableName = (String) valueMap.get("tableName");
        String year = (String) valueMap.get("year");
        String month = (String) valueMap.get("month");
        String day = (String) valueMap.get("day");
        String hour = (String) valueMap.get("hour");
        LOG.info("add partition start..., table: {}, year: {}, month: {}, day: {}, hour: {}", tableName, year, month, day, hour);
        String partitionSql = this.templateEngine.process(templateName, new Context(null, valueMap));
        LOG.debug("add partition sql:\n {}", partitionSql);
        String resultDirOnS3 = getAthenaQueryResultDirOnS3(year, month, day);
        String queryExecutionId = athenaExecutor.submitAthenaQuery(athenaDatabase, partitionSql, resultDirOnS3);
        athenaExecutor.waitForQueryToComplete(queryExecutionId);
        LOG.info("add partition complete, table: {}, year: {}, month: {}, day: {}, hour: {}, id: {}, result path: {} ", tableName, year, month, day, hour, queryExecutionId, resultDirOnS3);
    }

    private String getAthenaQueryResultDirOnS3(String year, String month, String day) {
        return String.join("/", "s3:/", awsConfig.getS3Bucket(), AthenaConstants.DATA_PATH_ATHENA, year, month, day);
    }

    private void compress2Zip(Path parentZipPath, File dataFile) {
        Path zipFilePath = Paths.get(parentZipPath.toAbsolutePath().toString(), dataFile.getName() + ".gz");
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
}
