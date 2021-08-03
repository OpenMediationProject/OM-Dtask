// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud;

public interface CloudService {
    /**
     * backfill collect data, hourly within give time (beginDateHour <= the time  < endDateHour)
     *
     * @param tableName     tablename
     * @param beginDateHour begin date and hour, format: yyyyMMddHH
     * @param endDateHour   end date and hour , format: yyyyMMddHH
     */
    void backfillCollectData(String tableName, String beginDateHour, String endDateHour);


    /**
     * backfill common report data, hourly within give time (beginDateHour <= the time  < endDateHour)
     *
     * @param beginDateHour begin date and hour, format: yyyyMMddHH
     * @param endDateHour   end date and hour , format: yyyyMMddHH
     */
    void backfillCommonReport(String beginDateHour, String endDateHour);


    /**
     * backfill cp report data, hourly within give time (beginDateHour <= the time  < endDateHour)
     *
     * @param beginDateHour begin date and hour, format: yyyyMMddHH
     * @param endDateHour   end date and hour , format: yyyyMMddHH
     */
    void backfillCpReport(String beginDateHour, String endDateHour);

    /**
     * backfill user report data, hourly within give date (beginDate <= the time  < endDate)
     *
     * @param beginDate begin date, format: yyyyMMdd
     * @param endDate   end date, format: yyyyMMdd
     */
    void backfillUserReport(String beginDate, String endDate);

    /**
     * backfill user ad revenue data, hourly within give date (beginDate <= the time  < endDate)
     *
     * @param beginDate begin date, format: yyyyMMdd
     * @param endDate   end date, format: yyyyMMdd
     */
    void backfillUserAdRevenue(String beginDate, String endDate);

    /**
     * backfill publisher user data, hourly within give date (beginDate <= the time  < endDate)
     * plealse
     *
     * @param beginDate begin date, format: yyyyMMdd
     * @param endDate   end date, format: yyyyMMdd
     */
    void backfillPublisherUser(String beginDate, String endDate);

    /**
     * backfill ltv report, hourly within give date (beginDate <= the time  < endDate)
     *
     * @param beginDate begin date, format: yyyyMMdd
     * @param endDate   end date, format: yyyyMMdd
     */
    void backfillLtvReport(String beginDate, String endDate);
}
