
# [20201201]

## 功能变更

1. cp 相关统计
2. deu 相关统计
3. lr 数据结构优化
4. 价格相关数据精度调整

## 升级步骤

### 1. 停止 om-dtask 程序
### 2. 需要删除 athena 中的 lr / iap / ods_stat_adnetwork 表,  登陆到 athena 的查询页面上执行:

```sql
drop table if EXISTS lr;
drop table if EXISTS iap;
drop table if EXISTS ods_stat_adnetwork;
```

> 注意: 系统重启后,会重新构建这些表, 其中 lr, iap 表需要手动挂载最近5天的数据

### 3. mysql 中新增表, 在 mysql 执行

```sql
--  (如需开启更细粒度的 dau ,请将字典表对应开关值设置成 "1")
INSERT INTO open_mediation.om_dict (pid, name, value, descn)
VALUES (100, 'dau_dimensions', '{"instance":0, "placement":0, "adn_placement":0, "adn":0}', '多维度Dau控制, key: 维度, value: [0, 1], 0:关闭,1:开启 ');


CREATE TABLE IF NOT EXISTS `stat_cp`
(
    `id`           int(11) UNSIGNED    NOT NULL AUTO_INCREMENT,
    `day`          date                NOT NULL                           DEFAULT '0000-00-00' COMMENT 'timezone: UTC',
    `hour`         tinyint(2)          NOT NULL                           DEFAULT '0',
    `publisher_id` int(10) UNSIGNED    NOT NULL                           DEFAULT '0',
    `pub_app_id`   int(10) UNSIGNED    NOT NULL                           DEFAULT '0',
    `placement_id` int(10) UNSIGNED    NOT NULL                           DEFAULT '0',
    `country`      varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin   DEFAULT NULL COMMENT 'country a2',
    `app_id`       varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '',
    `campaign_id`  int(10) UNSIGNED    NOT NULL                           DEFAULT '0',
    `creative_id`  int(10) UNSIGNED    NOT NULL                           DEFAULT '0',
    `impr`         bigint(10) UNSIGNED NOT NULL                           DEFAULT '0' COMMENT '展现数',
    `click`        bigint(10) UNSIGNED NOT NULL                           DEFAULT '0' COMMENT '点击数',
    `win_price`    decimal(16, 6)      NOT NULL                           DEFAULT '0.000000' COMMENT 'CPM',
    `create_time`  timestamp           NOT NULL                           DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`, `day`),
    KEY `day` (`day`),
    KEY `publisher_id` (`publisher_id`),
    KEY `pub_app_id` (`pub_app_id`),
    KEY `placement_id` (`placement_id`)
) COMMENT ='stat cp, partition by day'
    /*!50100 PARTITION BY RANGE (to_days(`day`))
    (PARTITION p20201201 VALUES LESS THAN (738126) ENGINE = InnoDB) */
;

CREATE TABLE IF NOT EXISTS `stat_dau_adn`
(
    `id`           int(11) UNSIGNED    NOT NULL AUTO_INCREMENT,
    `day`          date                NOT NULL COMMENT 'timezone: UTC',
    `publisher_id` int(10) UNSIGNED             DEFAULT '0' COMMENT 'publisher.id',
    `pub_app_id`   int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'publisher_app.id',
    `platform`     tinyint(2) UNSIGNED NOT NULL COMMENT '0:iOS,1:Android',
    `country`      varchar(4)                   DEFAULT NULL COMMENT 'Country a2',
    `adn_id`       int(10) UNSIGNED             DEFAULT '0' COMMENT 'Adnetwork id',
    `ip_count`     int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'ip的个数',
    `did_count`    int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'gaid or idfa 的个数',
    `dau`          int(10) UNSIGNED    NOT NULL DEFAULT '0',
    `deu`          int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT '当日打开了App且观看了广告的人数',
    `create_time`  timestamp           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`, `day`),
    KEY `day` (`day`),
    KEY `publisher_id` (`publisher_id`),
    KEY `pub_app_id` (`pub_app_id`)
) COMMENT ='DAU & DEU, partition by day'
    /*!50100 PARTITION BY RANGE (to_days(`day`))
    (PARTITION p20201201 VALUES LESS THAN (738126) ENGINE = InnoDB) */
;
			    
CREATE TABLE IF NOT EXISTS `stat_dau_placement`
(
    `id`           int(11) UNSIGNED    NOT NULL AUTO_INCREMENT,
    `day`          date                NOT NULL COMMENT 'timezone: UTC',
    `publisher_id` int(10) UNSIGNED             DEFAULT '0' COMMENT 'publisher.id',
    `pub_app_id`   int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'publisher_app.id',
    `platform`     tinyint(2) UNSIGNED NOT NULL COMMENT '0:iOS,1:Android',
    `country`      varchar(4)                   DEFAULT NULL COMMENT 'Country a2',
    `placement_id` int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'placement id',
    `adn_id`       int(10) UNSIGNED             DEFAULT '0' COMMENT 'Adnetwork id',
    `ip_count`     int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'ip的个数',
    `did_count`    int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'gaid or idfa 的个数',
    `dau`          int(10) UNSIGNED    NOT NULL DEFAULT '0',
    `deu`          int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT '当日打开了App且观看了广告的人数',
    `create_time`  timestamp           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`, `day`),
    KEY `day` (`day`),
    KEY `publisher_id` (`publisher_id`),
    KEY `pub_app_id` (`pub_app_id`)
) COMMENT ='DAU & DEU, partition by day'
    /*!50100 PARTITION BY RANGE (to_days(`day`))
    (PARTITION p20201201 VALUES LESS THAN (738126) ENGINE = InnoDB) */
;
 
CREATE TABLE IF NOT EXISTS `stat_dau_instance`
(
    `id`           int(11) UNSIGNED    NOT NULL AUTO_INCREMENT,
    `day`          date                NOT NULL COMMENT 'timezone: UTC',
    `publisher_id` int(10) UNSIGNED             DEFAULT '0' COMMENT 'publisher.id',
    `pub_app_id`   int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'publisher_app.id',
    `platform`     tinyint(2) UNSIGNED NOT NULL COMMENT '0:iOS,1:Android',
    `country`      varchar(4)                   DEFAULT NULL COMMENT 'Country a2',
    `placement_id` int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'placement id',
    `instance_id`  int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'instance id',
    `adn_id`       int(10) UNSIGNED             DEFAULT '0' COMMENT 'Adnetwork id',
    `ip_count`     int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'ip的个数',
    `did_count`    int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'gaid or idfa 的个数',
    `dau`          int(10) UNSIGNED    NOT NULL DEFAULT '0',
    `deu`          int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT '当日打开了App且观看了广告的人数',
    `create_time`  timestamp           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`, `day`),
    KEY `day` (`day`),
    KEY `publisher_id` (`publisher_id`),
    KEY `pub_app_id` (`pub_app_id`)
) COMMENT ='DAU & DEU, partition by day'
    /*!50100 PARTITION BY RANGE (to_days(`day`))
    (PARTITION p20201201 VALUES LESS THAN (738126) ENGINE = InnoDB) */
;		


CREATE TABLE IF NOT EXISTS `stat_dau_adn_placement`
(
    `id`           int(11) UNSIGNED    NOT NULL AUTO_INCREMENT,
    `day`          date                NOT NULL COMMENT 'timezone: UTC',
    `publisher_id` int(10) UNSIGNED             DEFAULT '0' COMMENT 'publisher.id',
    `pub_app_id`   int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'publisher_app.id',
    `platform`     tinyint(2) UNSIGNED NOT NULL COMMENT '0:iOS,1:Android',
    `country`      varchar(4)                   DEFAULT NULL COMMENT 'Country a2',
    `placement_id` int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'placement id',
    `adn_id`       int(10) UNSIGNED             DEFAULT '0' COMMENT 'Adnetwork id',
    `ip_count`     int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'ip的个数',
    `did_count`    int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT 'gaid or idfa 的个数',
    `dau`          int(10) UNSIGNED    NOT NULL DEFAULT '0',
    `deu`          int(10) UNSIGNED    NOT NULL DEFAULT '0' COMMENT '当日打开了App且观看了广告的人数',
    `create_time`  timestamp           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`, `day`),
    KEY `day` (`day`),
    KEY `publisher_id` (`publisher_id`),
    KEY `pub_app_id` (`pub_app_id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 131
  DEFAULT CHARSET = utf8 COMMENT ='DAU & DEU, partition by day'
    /*!50100 PARTITION BY RANGE (to_days(`day`))
    (PARTITION p20201201 VALUES LESS THAN (738126) ENGINE = InnoDB) */
```

### 4. 重启系统 

- a. 重新挂载数据: 访问 Backfill 接口, 挂载 lr / iap 表最近 5 天的数据

    ```bash
    curl "http://{om-dtask-server-host}:{port}/dc/backfill/collect/data?tableName=lr&beginDateHour={yyyyMMddHH}&endDateHour={yyyyMMddHH}"
    
    curl "http://{om-dtask-server-host}:{port}/dc/backfill/collect/data?tableName=iap&beginDateHour={yyyyMMddHH}&endDateHour={yyyyMMddHH}"
    ```

    例如:

    ```bash
    curl "http://127.0.0.1:8080/dc/backfill/collect/data?tableName=lr&beginDateHour=20201201&endDateHour=20201206"
    
    curl "http://127.0.0.1:8080/dc/backfill/collect/data?tableName=iap&beginDateHour=20201201&endDateHour=20201206"
    ```

- d.补录报表数据:  如果发现缺少某个小时的报表数据, 等待上一步结束后, 可以 backfill 相关小时的报表数据

    ```bash
    curl "http://{om-dtask-server-host}:{port}/dc/backfill/common/report?beginDateHour={yyyyMMddHH}&endDateHour={yyyyMMddHH}"
    ```

---

# [20201107]
### 1. 停止 om-dtask 程序

### 2. 需要删除 athena 中的 ods_stat_adnetwork 表,  登陆到 athena 的查询页面上执行:

```sql
drop table if EXISTS ods_stat_adnetwork;
```

> 注意: 此表结构有所变化, 系统启动后会重新建立此表,历史数据无需 backfill

### 3. mysql 中, 新增字典数据 及 stat_user_ltv 表,  在 mysql 执行: 

```sql
--  (如需开启 UAR ,请将字典表开关值设置成 "1")
INSERT INTO om_dict (pid, name, value, descn) VALUES (100, 'uar_switch', 0, '计算UAR的开关,0:关闭,1:开启');

--  (如需开启 LVT ,请将字典表开关值设置成 "1")
INSERT INTO om_dict (pid, name, value, descn) VALUES (100, 'ltv_switch', 0, '计算LTV的开关,0:关闭,1:开启');

--  (计算LTV的时间跨度, 单位天, 开启 LVT 后生效)
INSERT INTO om_dict (pid, name, value, descn) VALUES (100, 'ltv_date_range', 30, '计算LTV的时间跨度, 单位天');

-- lvt 报表
 CREATE TABLE stat_user_ltv (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `day` date NOT NULL DEFAULT '0000-00-00' COMMENT 'timezone: UTC',
 `base_date` date NOT NULL,
 `retention_day` int(10) unsigned NOT NULL DEFAULT '0',
 `country` varchar(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT 'Country a2',
 `publisher_id` int(10) unsigned DEFAULT '0' COMMENT 'publisher.id',
 `pub_app_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'publisher_app.id',
 `user_cnt_new` bigint(20) unsigned NOT NULL DEFAULT '0',
 `user_cnt_old` bigint(20) unsigned NOT NULL DEFAULT '0',
 `retention_cnt_new` bigint(20) unsigned NOT NULL DEFAULT '0',
 `retention_cnt_old` bigint(20) unsigned NOT NULL DEFAULT '0',
 `mediation_value_new` decimal(16,6) NOT NULL DEFAULT '0.000000',
 `mediation_value_old` decimal(16,6) NOT NULL DEFAULT '0.000000',
 `iap_value_new` decimal(16,6) NOT NULL DEFAULT '0.000000',
 `iap_value_old` decimal(16,6) NOT NULL DEFAULT '0.000000', 
 `total_value_new` decimal(16,6) NOT NULL DEFAULT '0.000000',
 `total_value_old` decimal(16,6) NOT NULL DEFAULT '0.000000',
 `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`,`day`),
 KEY `day` (`day`),
 KEY `publisher_id` (`publisher_id`),
 KEY `pub_app_id` (`pub_app_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='user ltv, partition by day'
 /*!50100 PARTITION BY RANGE (to_days(`day`))
 (PARTITION p20201101 VALUES LESS THAN (737838) ENGINE = InnoDB) */
 ;
```

### 4. 启动系统
重启系统后, 升级完成.

### 5. 解决当 lr 表数据, 在意外重启服务等情况会导致 json 格式数据错误, 会导致 athena 中 LR 表相关分区数据无法读取的问题

解决方案: 需要删除 lr 表, 并重新挂载分区来加载历史数据, 至少加载 2 天的数据:

- a. 需要删除 athena 中的 lr 表, 登陆到 athena 的查询页面上执行:

    ```sql
    drop table if EXISTS lr;
    ```

- b. 重启系统
    > 注意: 系统启动后会自动重建 lr 表
    
- c. 系统启动后, 访问 Backfill 接口, 挂载最近 2 天的数据
    
    ```bash
    curl "http://{om-dtask-server-host}:{port}/dc/backfill/collect/data?tableName=lr&beginDateHour={yyyyMMddHH}&endDateHour={yyyyMMddHH}"
    ```

- d. 如果发现缺少某个小时的报表数据, 等待上一步结束后, 可以 backfill 相关小时的报表数据

    ```bash
    curl "http://{om-dtask-server-host}:{port}/dc/backfill/common/report?beginDateHour={yyyyMMddHH}&endDateHour={yyyyMMddHH}"
    ```
