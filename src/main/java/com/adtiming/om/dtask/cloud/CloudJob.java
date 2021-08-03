// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud;

import com.adtiming.om.dtask.util.Constants;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;

public interface CloudJob {
    Logger LOG = LogManager.getLogger();

    void collectDatas(LocalDateTime executeDateTime);

    void collectDatas(String tableName, LocalDateTime executeDateTime);

    void commonReport(LocalDateTime executeDateTime);

    void cpReport(LocalDateTime executeDateTime);

    void pubAppCountryReport(LocalDate executeDate);

    void userReport(LocalDate executeDate);

    void userAdRevenue(LocalDate executeDate);

    void collectDwsPublisherUser(LocalDate executeDate);

    void ltvReport(LocalDate executeDate);

    default boolean isSwitchOn(int switchValue) {
        return CloudConstants.SWITCH_ON == switchValue;
    }

    default void clearTmpLocalDataDirectory(String dataDirectory, LocalDate executeDate) {
        Path tmpLocalPath = Paths.get(dataDirectory, executeDate.format(Constants.FORMATTER_YYYYMMDD));
        try {
            LOG.info("clear tmp local data directory start... : {}", tmpLocalPath);
            FileUtils.deleteDirectory(new File(tmpLocalPath.toUri()));
            LOG.info("clear tmp local data directory complete : {}", tmpLocalPath);
        } catch (IOException e) {
            LOG.error("clear tmp local data directory, directory: {}", tmpLocalPath, e);
        }
    }
}
