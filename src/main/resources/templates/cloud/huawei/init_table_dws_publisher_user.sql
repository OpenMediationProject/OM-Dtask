CREATE TABLE IF NOT EXISTS dws_publisher_user
(
    publisher_id int,
    pub_app_id   int,
    country      string,
    uid          string,
    is_new       int         COMMENT '0: old, 1: new',
    ymd          string      COMMENT '时间分区字段:yyyymmdd'
)
USING orc
PARTITIONED BY (ymd)
LOCATION 'obs://[(${dliBucket})]/[(${tableDataPath})]/dws_publisher_user/'
;