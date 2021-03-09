SELECT date_format(from_unixtime(serverTs / 1000, 'UTC'), '%Y-%m-%d') AS day,
       coalesce(publisherId, 0)                                       AS publisher_id,
       coalesce(pubAppId, 0)                                          AS pub_app_id,
       coalesce(plat, -1)                                             AS platform,
       country,
       appv                                                           AS app_version,
       sdkv                                                           AS sdk_version,
       osv                                                            AS os_version,

       count(DISTINCT ip)                                             AS ip_count,
       count(DISTINCT did)                                            AS did_count,
       count(DISTINCT uid)                                            AS dau,
       count(DISTINCT if(type = 6, uid, '')) - 1                      AS due
FROM lr
WHERE
    serverTs IS NOT NULL
    AND y='[(${year})]'
    AND m='[(${month})]'
    AND d='[(${day})]'
GROUP BY
    date_format(from_unixtime(serverTs / 1000, 'UTC'), '%Y-%m-%d'),
    coalesce(publisherId, 0),
    coalesce(pubAppId, 0),
    country,
    coalesce(plat, -1),
    appv,
    sdkv,
    osv
;