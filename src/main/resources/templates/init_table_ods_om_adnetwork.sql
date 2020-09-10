CREATE EXTERNAL TABLE IF NOT EXISTS open_mediation.ods_om_adnetwork
(
    id                 int,
    name               string,
    class_name         string COMMENT 'SDK className prefix',
    descn              string COMMENT 'desc'
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'serialization.format' = '\u0001',
        'field.delim' = '\u0001'
        ) LOCATION 's3://[(${s3Bucket})]/[(${tableDataPath})]/ods_om_adnetwork/'
    TBLPROPERTIES ('has_encrypted_data' = 'false')

;