INSERT INTO report_cp
SELECT NULL                                   AS id,
       FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd') AS day,
       FROM_UNIXTIME(ts / 1000, 'HH')         AS hour,
       NVL(publisherId, 0)                    AS publisher_id,
       NVL(pubAppId, 0)                       AS pub_app_id,
       NVL(pid, 0)                            AS placement_id,
       NVL(cy, '')                            AS country,
       NVL(appId, '')                         AS app_id,
       NVL(cid, 0)                            AS campaign_id,
       NVL(crid, 0)                           AS creative_id,

       SUM(NVL(impr, 0))                      AS impr,
       SUM(NVL(click, 0))                     AS click,
       SUM(NVL(cost, 0))                      AS win_price,
       NULL                                   AS create_time
FROM cptk
WHERE ts IS NOT NULL
  AND y = '[(${year})]'
  AND m = '[(${month})]'
  AND d = '[(${day})]'
  AND h = '[(${hour})]'
GROUP BY FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd'),
    FROM_UNIXTIME(ts / 1000, 'HH'),
    NVL(publisherId, 0),
    NVL(pubAppId, 0),
    NVL(pid, 0),
    NVL(cy, ''),
    NVL(appId, ''),
    NVL(cid, 0),
    NVL(crid, 0)
;