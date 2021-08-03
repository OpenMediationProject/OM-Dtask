SELECT
       pub_app_id,
       device_id,
       device_type,
       ods_om_adnetwork.class_name AS adn_name,
       impr_cnt,
       click_cnt,
       revenue
FROM
    (
        SELECT
               mediation_data.pub_app_id                                                                                                                        AS pub_app_id,
               mediation_data.device_id                                                                                                                         AS device_id,
               if(mediation_data.platform = 0, 'IDFA', 'GAID')                                                                                                  AS device_type,
               mediation_data.adn_id                                                                                                                            AS adn_id,

               sum(impr_cur)                                                                                                                                        AS impr_cnt,
               sum(click_cur)                                                                                                                                       AS click_cnt,
               sum(IF(coalesce(cost, 0) = 0 OR impr_total + click_total = 0, 0, ((impr_cur + 4 * click_cur) * coalesce(cost, 0)) / (impr_total + 4 * click_total)))     AS revenue

        FROM (
                 SELECT coalesce(pubAppId, 0)                                                                                                                                                    AS pub_app_id,
                        coalesce(plat, -1)                                                                                                                                                       AS platform,
                        coalesce(mid, 0)                                                                                                                                                         AS adn_id,
                        country,
                        coalesce(iid, 0)                                                                                                                                                         AS instance_id,
                        coalesce(did, '')                                                                                                                                                        AS device_id,

                        SUM(if(type = 6 OR impr > 0, coalesce(impr, 1), 0))                                                                                                                      AS impr_cur,
                        SUM(if(type = 7 OR click > 0, coalesce(click, 1), 0))                                                                                                                    AS click_cur,
                        SUM(SUM(if(type = 6 OR impr > 0, coalesce(impr, 1), 0))) OVER (PARTITION BY coalesce(pubAppId, 0), coalesce(plat, -1), coalesce(mid, 0), country, coalesce(iid, 0))      AS impr_total,
                        SUM(SUM(if(type = 7 OR click > 0, coalesce(click, 1), 0))) OVER (PARTITION BY coalesce(pubAppId, 0), coalesce(plat, -1), coalesce(mid, 0), country, coalesce(iid, 0))    AS click_total
                 FROM lr
                 WHERE
                    serverTs IS NOT NULL
                    AND y='[(${year})]'
                    AND m='[(${month})]'
                    AND d='[(${day})]'
                 GROUP BY coalesce(pubAppId, 0),
                          coalesce(plat, -1),
                          coalesce(mid, 0),
                          country,
                          coalesce(iid, 0),
                          coalesce(did, '')
             ) AS mediation_data

                 LEFT JOIN
             (
                 SELECT pub_app_id,
                        platform,
                        adn_id,
                        country,
                        instance_id,
                        SUM(coalesce(cost, 0)) cost
                 FROM ods_stat_adnetwork

                 WHERE
                    y='[(${year})]'
                    AND m='[(${month})]'
                    AND d='[(${day})]'
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
                 if(mediation_data.platform = 0, 'IDFA', 'GAID'),
                 mediation_data.adn_id
    ) AS final_data
    LEFT JOIN ods_om_adnetwork  ON final_data.adn_id = ods_om_adnetwork.id
;