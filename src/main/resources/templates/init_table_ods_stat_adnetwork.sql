CREATE EXTERNAL TABLE IF NOT EXISTS open_mediation.ods_stat_adnetwork
(
    id                 int,
    pub_app_id         int,
    platform           tinyint COMMENT '0:ios, 1:android',
    adn_id             int COMMENT 'AdNetwork ID',
    country            string COMMENT 'alpha2',
    instance_id        int,
    cost               decimal(16, 4) COMMENT ' COST '
) PARTITIONED BY (
    y string,
    m string,
    d string
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'serialization.format' = '\u0001',
        'field.delim' = '\u0001'
        ) LOCATION 's3://[(${s3Bucket})]/[(${tableDataPath})]/ods_stat_adnetwork/'
    TBLPROPERTIES ('has_encrypted_data' = 'false')

;