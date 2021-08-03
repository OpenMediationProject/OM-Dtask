CREATE EXTERNAL TABLE IF NOT EXISTS dws_publisher_user
(
    `publisher_id` int,
    `pub_app_id`   int,
    `country`      string,
    `uid`          string,
    `is_new`       int COMMENT '0: old, 1: new'
) PARTITIONED BY ( ymd string )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'serialization.format' = ',',
        'field.delim' = ','
        ) LOCATION 's3://[(${s3Bucket})]/[(${tableDataPath})]/dws_publisher_user/'
    TBLPROPERTIES ('has_encrypted_data' = 'false');