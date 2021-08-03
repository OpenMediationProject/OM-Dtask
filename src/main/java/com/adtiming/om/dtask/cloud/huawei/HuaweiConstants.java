// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud.huawei;

import com.adtiming.om.dtask.cloud.CloudConstants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class HuaweiConstants {
    public static final String TABLE_NAME_REPORT_COMMON = "report_common";
    public static final String TABLE_NAME_REPORT_CP = "report_cp";
    public static final String TABLE_NAME_REPORT_LTV = "report_ltv";
    public static final String TABLE_NAME_REPORT_UAR = "report_uar";
    public static final String TABLE_NAME_REPORT_USER = "report_user";
    public static final String TABLE_NAME_REPORT_USER_ADN = "report_user_adn";
    public static final String TABLE_NAME_REPORT_USER_ADN_PLACEMENT = "report_user_adn_placement";
    public static final String TABLE_NAME_REPORT_USER_INSTANCE = "report_user_instance";
    public static final String TABLE_NAME_REPORT_USER_PLACEMENT = "report_user_placement";
    public static final String TABLE_NAME_REPORT_PUBAPP_COUNTRY = "report_pubapp_country";

    public static final List<String> TABLE_NAMES;

    static {
        List<String> a = new ArrayList<>(CloudConstants.TABLE_NAMES.size() + 2);
        a.addAll(CloudConstants.TABLE_NAMES);
        a.add(TABLE_NAME_REPORT_COMMON);
        a.add(TABLE_NAME_REPORT_CP);
        a.add(TABLE_NAME_REPORT_LTV);
        a.add(TABLE_NAME_REPORT_UAR);
        a.add(TABLE_NAME_REPORT_USER);
        a.add(TABLE_NAME_REPORT_USER_ADN);
        a.add(TABLE_NAME_REPORT_USER_ADN_PLACEMENT);
        a.add(TABLE_NAME_REPORT_USER_INSTANCE);
        a.add(TABLE_NAME_REPORT_USER_PLACEMENT);
        a.add(TABLE_NAME_REPORT_PUBAPP_COUNTRY);
        TABLE_NAMES = Collections.unmodifiableList(a);
    }

    private HuaweiConstants() {
    }
}
















