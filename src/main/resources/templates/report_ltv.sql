SELECT
       '[(${executeYear})][(${executeMonth})][(${executeDay})]'                                                                         AS execute_date,
       user_info_data.ymd                                                                                                               AS base_date,
       date_diff('day',
                 from_iso8601_date('[(${executeYear})]-[(${executeMonth})]-[(${executeDay})]'),
                 from_iso8601_date(
                         CONCAT(SUBSTR(user_info_data.ymd, 1, 4), '-', SUBSTR(user_info_data.ymd, 5, 2), '-', SUBSTR(user_info_data.ymd, 7, 2))
                     )
           )                                                                                                                            AS retention_day,

       user_info_data.country,
       user_info_data.publisher_id,
       user_info_data.pub_app_id,
       SUM(IF(user_info_data.is_new = 1, 1, 0))                                                                                         AS user_cnt_new,
       SUM(IF(user_info_data.is_new = 0, 1, 0))                                                                                         AS user_cnt_old,
       SUM(IF(mediation_value_data.uid IS NOT NULL AND user_info_data.is_new = 1, 1, 0))                                                AS retention_cnt_new,
       SUM(IF(mediation_value_data.uid IS NOT NULL AND user_info_data.is_new = 0, 1, 0))                                                AS retention_cnt_old,
       SUM(IF(user_info_data.is_new = 1, coalesce(mediation_value_data.mediation_value, 0), 0))                                         AS mediation_value_new,
       SUM(IF(user_info_data.is_new = 0, coalesce(mediation_value_data.mediation_value, 0), 0))                                         AS mediation_value_old,
       SUM(IF(user_info_data.is_new = 1, coalesce(iap_value_data.iap_value, 0), 0))                                                     AS iap_value_new,
       SUM(IF(user_info_data.is_new = 0, coalesce(iap_value_data.iap_value, 0), 0))                                                     AS iap_value_old,
       SUM(IF(user_info_data.is_new = 1, coalesce(mediation_value_data.mediation_value, 0) + coalesce(iap_value_data.iap_value, 0), 0)) AS total_value_new,
       SUM(IF(user_info_data.is_new = 0, coalesce(mediation_value_data.mediation_value, 0) + coalesce(iap_value_data.iap_value, 0), 0)) AS total_value_old

FROM (
         SELECT ymd,
                publisher_id,
                pub_app_id,
                country,
                uid,
                is_new
         FROM dws_publisher_user
         WHERE ymd >= '[(${beginYear})][(${beginMonth})][(${beginDay})]'
           AND ymd <= '[(${executeYear})][(${executeMonth})][(${executeDay})]'
     ) AS user_info_data

         LEFT JOIN

     (
         SELECT country,
                publisher_id,
                pub_app_id,
                uid,
                SUM(mediation_value) AS mediation_value
         FROM (
                  SELECT user_behavior_data.country                                                                                                              AS country,
                         user_behavior_data.publisher_id                                                                                                         AS publisher_id,
                         user_behavior_data.pub_app_id                                                                                                           AS pub_app_id,
                         uid,
                         IF(coalesce(cost, 0) = 0 OR impr_total + click_total = 0, 0, ((impr + 4 * click) * coalesce(cost, 0)) / (impr_total + 4 * click_total)) AS mediation_value
                  FROM (
                           SELECT country,
                                  publisherId                                                                          AS publisher_id,
                                  pubAppId                                                                             AS pub_app_id,
                                  mid                                                                                  AS adn_id,
                                  iid                                                                                  AS instance_id,
                                  uid,
                                  SUM(if(type = 6, 1, 0)) OVER (PARTITION BY country, publisherId, pubAppId, mid, iid) AS impr_total,
                                  SUM(if(type = 7, 1, 0)) OVER (PARTITION BY country, publisherId, pubAppId, mid, iid) AS click_total,
                                  if(type = 6, 1, 0)                                                                   AS impr,
                                  if(type = 7, 1, 0)                                                                   AS click
                           FROM lr
                           WHERE y = '[(${executeYear})]'
                             AND m = '[(${executeMonth})]'
                             AND d = '[(${executeDay})]'
                             AND country IS NOT NULL
                             AND publisherId IS NOT NULL
                             AND pubAppId IS NOT NULL
                             AND mid IS NOT NULL
                             AND iid IS NOT NULL
                             AND uid IS NOT NULL
                       ) AS user_behavior_data
                           LEFT JOIN
                       (
                           SELECT country,
                                  publisher_id,
                                  pub_app_id,
                                  adn_id,
                                  instance_id,
                                  SUM(coalesce(cost, 0)) cost
                           FROM ods_stat_adnetwork
                           WHERE y = '[(${executeYear})]'
                             AND m = '[(${executeMonth})]'
                             AND d = '[(${executeDay})]'
                           GROUP BY country, publisher_id, pub_app_id, adn_id, instance_id
                       ) AS total_mediation_value_data
                       ON
                               user_behavior_data.country = total_mediation_value_data.country
                               AND user_behavior_data.publisher_id = total_mediation_value_data.publisher_id
                               AND user_behavior_data.pub_app_id = total_mediation_value_data.pub_app_id
                               AND user_behavior_data.adn_id = total_mediation_value_data.adn_id
                               AND user_behavior_data.instance_id = total_mediation_value_data.instance_id
              ) AS detailed_mediation_value_data
         GROUP BY country,
                  publisher_id,
                  pub_app_id,
                  uid
     ) AS mediation_value_data
     ON
             user_info_data.country = mediation_value_data.country
             AND user_info_data.publisher_id = mediation_value_data.publisher_id
             AND user_info_data.pub_app_id = mediation_value_data.pub_app_id
             AND user_info_data.uid = mediation_value_data.uid


         LEFT JOIN
     (
         SELECT country,
                publisherId AS publisher_id,
                pubAppId    AS pub_app_id,
                uid,
                sum(iapUsd) AS iap_value
         FROM iap
         WHERE y = '[(${executeYear})]'
           AND m = '[(${executeMonth})]'
           AND d = '[(${executeDay})]'
           AND country IS NOT NULL
           AND publisherId IS NOT NULL
           AND pubAppId IS NOT NULL
           AND uid IS NOT NULL
         GROUP BY country,
                  publisherId,
                  pubAppId,
                  uid
     ) AS iap_value_data
     ON
             user_info_data.country = iap_value_data.country
             AND user_info_data.publisher_id = iap_value_data.publisher_id
             AND user_info_data.pub_app_id = iap_value_data.pub_app_id
             AND user_info_data.uid = iap_value_data.uid
GROUP BY user_info_data.ymd,
         user_info_data.country,
         user_info_data.publisher_id,
         user_info_data.pub_app_id
;
