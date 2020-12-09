package com.adtiming.om.dtask.dto;

public interface CpCampaignTarget {

    int PUBAPP_WHITE = 0;         // PublisherApp 白名单, 值为单个 publisher_app.id,
    int PUBAPP_BLACK = 1;         // PublisherApp 黑名单, 值为单个 publisher_app.id,
    int PLACEMENT_WHITE = 2;      // Placement 白名单, 值为单个 publisher_placment.id,
    int PLACEMENT_BLACK = 3;      // Placement 黑名单, 值为单个 publisher_placment.id,
    int MAKE_WHITE = 4;           // Make 白名单,
    int MAKE_BLACK = 5;           // Make 黑名单,
    int BRAND_WHITE = 6;          // Brand 白名单,
    int BRAND_BLACK = 7;          // Brand 黑名单,
    int MODEL_WHITE = 8;          // Model 白名单,
    int MODEL_BLACK = 9;          // Model 黑名单,
    int DEVICE_TYPE_WHITE = 10;   // DeviceType 白名单,
    int DEVICE_TYPE_BLACK = 11;   // DeviceType 黑名单,
    int CONNECTION_TYPE = 12;     // ConnectionType, 连接类型, 二进制, 从右到左(低位起) wifi, 2G, 3G, 4G,
    int MCCMNC_WHITE = 13;        // Mccmnc 白名单,
    int MCCMNC_BLACK = 14;        // Mccmnc 黑名单,
    int OSV_WHITE_EXP = 15;       // OSV 白名单表达式,
    int OSV_BLACK_EXP = 16;       // OSV 黑名单表达式,
    int COUNTRY_WHITE = 17;       // Country 白名单,
    int COUNTRY_BLACK = 18;       // Country 黑名单

}
