CREATE EXTERNAL TABLE IF NOT EXISTS cptk (
    cy             String            COMMENT 'Country, cy',
    make           String            COMMENT '设备产商',
    brand          String            COMMENT '设备品牌',
    model          String            COMMENT '设备型号',
    osv            String            COMMENT 'OS Version',
    appv           String            COMMENT 'app version',
    sdkv           String            COMMENT 'sdk version',
    reqid          String            COMMENT '请求ID',
    cts            bigint            COMMENT 'client 时间戳',
    sts            bigint            COMMENT 'server 时间戳',
    did            String            COMMENT 'devicdId',
    uid            String            COMMENT 'SDK 生成用户唯一标识',
    pid            int               COMMENT 'Placement ID',
    cid            bigint            COMMENT '活动ID',
    crid           bigint            COMMENT '创意ID',
    size           String            COMMENT 'ad size',
    bundle         String            COMMENT 'bundle',
    cont           int               COMMENT 'connection type',
    mccmnc         String            COMMENT 'mccmnc',
    carrier        String            COMMENT 'carrier, cr',
    lang           String            COMMENT 'language code',
    bidid          String            COMMENT 'bid id',
    tkid           String            COMMENT 'TrackID',
    click          int               COMMENT 'click',
    impr           int               COMMENT 'impression',
    ts             bigint            COMMENT '当前时间, 单位毫秒',
    snode          int               COMMENT 'Server Node ID',
    dcenter        int               COMMENT 'Server Dcenter ID',
    ip             String            COMMENT 'client IP',
    ua             String            COMMENT 'UserAgent',
    adPubId        int               COMMENT '活动所属 PublisherID',
    plat           int               COMMENT '0:iOS,1:Android',
    appId          String            COMMENT 'Campaign.appId',
    publisherId    int               COMMENT 'Placement.publisherId',
    pubAppId       int               COMMENT 'Placement.pubAppId',
    adType         int               COMMENT 'Placement.AdType',
    billingType    int               COMMENT 'Campaign.billingType',
    cost           decimal(20, 8)    COMMENT '根据 price 计算 cost'
) PARTITIONED BY (
    y string,
    m string,
    d string,
    h string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH
    SERDEPROPERTIES ('serialization.format' = '1', 'ignore.malformed.json' = 'true')
    LOCATION 's3://[(${s3Bucket})]/[(${tableDataPath})]/cptk/'
    TBLPROPERTIES ('has_encrypted_data'='false')
;
