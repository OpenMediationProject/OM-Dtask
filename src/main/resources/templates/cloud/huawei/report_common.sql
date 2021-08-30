INSERT INTO report_common
SELECT NULL                                                                       AS id,
       FROM_UNIXTIME(serverTs / 1000, 'yyyy-MM-dd')                               AS day,
       FROM_UNIXTIME(serverTs / 1000, 'HH')                                       AS hour,
       country,
       NVL(plat, -1)                                                              AS platform,
       NVL(publisherId, 0)                                                        AS publisher_id,
       NVL(pubAppId, 0)                                                           AS pub_app_id,
       NVL(pid, 0)                                                                AS placement_id,
       NVL(iid, 0)                                                                AS instance_id,
       NVL(scene, 0)                                                              AS scene_id,
       NVL(mid, 0)                                                                AS adn_id,
       NVL(abt, 0)                                                                AS abt,
       NVL(bid, 0)                                                                AS bid,
       NVL(ruleId, 0)                                                             AS rule_id,
       appv                                                                       AS app_version,
       sdkv                                                                       AS sdk_version,
       osv                                                                        AS os_version,

       SUM(IF(type = 2 OR wfReq > 0, NVL(wfReq, 1), 0))                           AS waterfall_request,
       SUM(IF(type = 3 OR wfFil > 0, NVL(wfFil, 1), 0))                           AS waterfall_filled,
       SUM(IF(type = 4 OR insReq > 0, NVL(insReq, 1), 0))                         AS instance_request,
       SUM(IF(type = 5 OR insFil > 0, NVL(insFil, 1), 0))                         AS instance_filled,
       SUM(IF(type = 8 OR vdStart > 0, NVL(vdStart, 1), 0))                       AS video_start,
       SUM(IF(type = 9 OR vdEnd > 0, NVL(vdEnd, 1), 0))                           AS video_complete,
       SUM(IF(type = 501 OR calledShow > 0, NVL(calledShow, 1), 0))               AS called_show,
       SUM(IF(type = 502 OR readyTrue > 0, NVL(readyTrue, 1), 0))                 AS is_ready_true,
       SUM(IF(type = 503 OR readyFalse > 0, NVL(readyFalse, 1), 0))               AS is_ready_false,
       SUM(IF(type = 7 OR click > 0, NVL(click, 1), 0))                           AS click,
       SUM(IF(type = 6 OR impr > 0, NVL(impr, 1), 0))                             AS impr,
       SUM(IF(type = 270 OR bidReq > 0, NVL(bidReq, 1), 0))                       AS bid_req,
       SUM(IF(type = 271 OR bidRes > 0, NVL(bidRes, 1), 0))                       AS bid_resp,
       SUM(IF(type = 271 OR bidResPrice > 0, NVL(bidResPrice, NVL(price, 0)), 0)) AS bid_resp_price,
       SUM(IF(type = 273 OR bidWin > 0, NVL(bidWin, 1), 0))                       AS bid_win,
       SUM(IF(type = 273 OR bidWinPrice > 0, NVL(bidWinPrice, NVL(price, 0)), 0)) AS bid_win_price,
       NULL                                                                       AS create_time
FROM lr
WHERE serverTs IS NOT NULL
  AND y = '[(${year})]'
  AND m = '[(${month})]'
  AND d = '[(${day})]'
  AND h = '[(${hour})]'
GROUP BY FROM_UNIXTIME(serverTs / 1000, 'yyyy-MM-dd'),
    FROM_UNIXTIME(serverTs / 1000, 'HH'),
    country,
    NVL(plat, -1),
    NVL(publisherId, 0),
    NVL(pubAppId, 0),
    NVL(pid, 0),
    NVL(iid, 0),
    NVL(scene, 0),
    NVL(mid, 0),
    NVL(abt, 0),
    NVL(bid, 0),
    NVL(ruleId, 0),
    appv,
    sdkv,
    osv
;