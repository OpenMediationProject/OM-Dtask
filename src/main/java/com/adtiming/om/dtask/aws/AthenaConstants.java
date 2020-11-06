// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.aws;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class AthenaConstants {

    public static final long SLEEP_AMOUNT_IN_MS = 1000;
    public static final int CLIENT_EXECUTION_TIMEOUT = 100000;
    public static final String DATA_PATH_ATHENA = "athena";
    public static final String DATA_PATH_TABLE = "om";
    public static final String TABLE_NAME_LR = "lr";
    public static final String TABLE_NAME_IAP = "iap";
    public static final String TABLE_NAME_ODS_OM_ADNETWORK = "ods_om_adnetwork";
    public static final String TABLE_NAME_ODS_STAT_ADNETWORK = "ods_stat_adnetwork";
    public static final String TABLE_NAME_DWS_PUBLISHER_USER = "dws_publisher_user";
    public static final List<String> TABLE_NAMES = ImmutableList.of(
            TABLE_NAME_LR,
            TABLE_NAME_IAP,
            TABLE_NAME_ODS_OM_ADNETWORK,
            TABLE_NAME_ODS_STAT_ADNETWORK,
            TABLE_NAME_DWS_PUBLISHER_USER
    );

}