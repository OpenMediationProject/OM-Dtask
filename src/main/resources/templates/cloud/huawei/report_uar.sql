INSERT OVERWRITE DIRECTORY 'obs://[(${dliBucket})]/[(${tableDataPath})]/report_uar/[(${year})]/[(${month})]/[(${day})]'
USING csv
SELECT pub_app_id,
       device_id,
       device_type,
       ods_om_adnetwork.class_name AS adn_name,
       impr_cnt,
       click_cnt,
       revenue
FROM (
         SELECT mediation_data.pub_app_id                                                                                                                  AS pub_app_id,
                mediation_data.device_id                                                                                                                   AS device_id,
                IF(mediation_data.platform = 0, 'IDFA', 'GAID')                                                                                            AS device_type,
                mediation_data.adn_id                                                                                                                      AS adn_id,

                SUM(impr_cur)                                                                                                                              AS impr_cnt,
                SUM(click_cur)                                                                                                                             AS click_cnt,
                SUM(IF(NVL(cost, 0) = 0 OR impr_total + click_total = 0, 0, ((impr_cur + 4 * click_cur) * NVL(cost, 0)) / (impr_total + 4 * click_total))) AS revenue

         FROM (
                  SELECT NVL(pubAppId, 0)                                                                                                                             AS pub_app_id,
                         NVL(plat, -1)                                                                                                                                AS platform,
                         NVL(mid, 0)                                                                                                                                  AS adn_id,
                         country,
                         NVL(iid, 0)                                                                                                                                  AS instance_id,
                         NVL(did, '')                                                                                                                                 AS device_id,

                         SUM(IF(type = 6 OR impr > 0, NVL(impr, 1), 0))                                                                                               AS impr_cur,
                         SUM(IF(type = 7 OR click > 0, NVL(click, 1), 0))                                                                                             AS click_cur,
                         SUM(SUM(IF(type = 6 OR impr > 0, NVL(impr, 1), 0))) OVER (PARTITION BY NVL(pubAppId, 0), NVL(plat, -1), NVL(mid, 0), country, NVL(iid, 0))   AS impr_total,
                          SUM(SUM(IF(type = 7 OR click > 0, NVL(click, 1), 0))) OVER (PARTITION BY NVL(pubAppId, 0), NVL(plat, -1), NVL(mid, 0), country, NVL(iid, 0)) AS click_total
                  FROM lr
                  WHERE serverTs IS NOT NULL
                    AND y = '[(${year})]'
                    AND m = '[(${month})]'
                    AND d = '[(${day})]'
                  GROUP BY NVL(pubAppId, 0),
                           NVL(plat, -1),
                           NVL(mid, 0),
                           country,
                           NVL(iid, 0),
                           NVL(did, '')
              ) AS mediation_data

                  LEFT JOIN
              (
                  SELECT pub_app_id,
                         platform,
                         adn_id,
                         country,
                         instance_id,
                         SUM(NVL(cost, 0)) cost
                  FROM ods_stat_adnetwork
                  WHERE day = '[(${year})]-[(${month})]-[(${day})]'
                  GROUP BY pub_app_id,
                      platform,
                      adn_id,
                      country,
                      instance_id
              )
                  AS adn_data
              ON
                          mediation_data.pub_app_id = adn_data.pub_app_id
                      AND mediation_data.platform = adn_data.platform
                      AND mediation_data.adn_id = adn_data.adn_id
                      AND mediation_data.country = adn_data.country
                      AND mediation_data.instance_id = adn_data.instance_id
         GROUP BY mediation_data.device_id,
                  mediation_data.pub_app_id,
                  IF(mediation_data.platform = 0, 'IDFA', 'GAID'),
                  mediation_data.adn_id
     ) AS final_data
         LEFT JOIN ods_om_adnetwork ON final_data.adn_id = ods_om_adnetwork.id
;