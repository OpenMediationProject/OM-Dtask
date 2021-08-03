// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class CloudConstants {

    public static final int SWITCH_ON = 1;

    public static final long SLEEP_AMOUNT_IN_MS = 1000;
    public static final int CLIENT_EXECUTION_TIMEOUT = 100000;
    public static final String DATA_PATH_TABLE = "om";
    public static final String TABLE_NAME_LR = "lr";
    public static final String TABLE_NAME_IAP = "iap";
    public static final String TABLE_NAME_CPTK = "cptk";
    public static final String TABLE_NAME_EVENT = "event";
    public static final String TABLE_NAME_ODS_OM_ADNETWORK = "ods_om_adnetwork";
    public static final String TABLE_NAME_ODS_STAT_ADNETWORK = "ods_stat_adnetwork";
    public static final String TABLE_NAME_DWS_PUBLISHER_USER = "dws_publisher_user";
    public static final List<String> TABLE_NAMES = Collections.unmodifiableList(Arrays.asList(
            TABLE_NAME_LR,
            TABLE_NAME_IAP,
            TABLE_NAME_CPTK,
            TABLE_NAME_EVENT,
            TABLE_NAME_ODS_OM_ADNETWORK,
            TABLE_NAME_ODS_STAT_ADNETWORK,
            TABLE_NAME_DWS_PUBLISHER_USER
    ));

    private CloudConstants() {
    }
}
