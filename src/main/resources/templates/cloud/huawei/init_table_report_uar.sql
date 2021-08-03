CREATE TABLE IF NOT EXISTS report_uar
(
    pub_app_id  int,
    device_id   string,
    device_type int,
    adn_name    string,
    impr_cnt    bigint,
    click_cnt   bigint,
    revenue     decimal(20, 8),
    ymd         string COMMENT '时间分区字段:yyyymmdd'

)
USING CSV
PARTITIONED BY (ymd)
LOCATION 'obs://[(${dliBucket})]/[(${tableDataPath})]/report_uar'
;