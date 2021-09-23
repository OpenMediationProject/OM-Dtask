INSERT INTO report_user_abtest
SELECT NULL                                         AS id,
       '[(${year})]-[(${month})]-[(${day})]'        AS day,
       NVL(publisherId, 0)                          AS publisher_id,
       NVL(pubAppId, 0)                             AS pub_app_id,
       NVL(plat, -1)                                AS platform,
       coalesce(abt, 0)                             AS abt,
       coalesce(abtId, 0)                          AS abt_id,
       country,
       appv                                         AS app_version,
       sdkv                                         AS sdk_version,
       osv                                          AS os_version,

       COUNT(DISTINCT ip)                           AS ip_count,
       COUNT(DISTINCT did)                          AS did_count,
       COUNT(DISTINCT uid)                          AS dau,
       COUNT(DISTINCT IF(type = 6, uid, '')) - 1    AS due,
       NULL                                         AS create_time
FROM lr
WHERE serverTs IS NOT NULL
  AND y = '[(${year})]'
  AND m = '[(${month})]'
  AND d = '[(${day})]'
GROUP BY
    NVL(publisherId, 0),
    NVL(pubAppId, 0),
    country,
    NVL(plat, -1),
    coalesce(abt, 0),
    coalesce(abtId, 0),
    appv,
    sdkv,
    osv
;