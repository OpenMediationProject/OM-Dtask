SELECT pubAppId,
       country,
       '[(${year})][(${month})][(${day})]' as day,
           CAST(SUM(IF(runk = floor(max_runk * 0.1), revenue, 0))/1000 AS DECIMAL(10,8)) AS top10_price,
           CAST(SUM(IF(runk = floor(max_runk * 0.2), revenue, 0))/1000 AS DECIMAL(10,8)) AS top20_price,
           CAST(SUM(IF(runk = floor(max_runk * 0.3), revenue, 0))/1000 AS DECIMAL(10,8)) AS top30_price,
           CAST(SUM(IF(runk = floor(max_runk * 0.4), revenue, 0))/1000 AS DECIMAL(10,8)) AS top40_price,
           CAST(SUM(IF(runk = floor(max_runk * 0.5), revenue, 0))/1000 AS DECIMAL(10,8)) AS top50_price
FROM (SELECT pubAppId,
    country,
    did,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY pubAppId, country ORDER BY revenue DESC) AS runk,
    COUNT() OVER (PARTITION BY pubAppId, country )                         AS max_runk
    FROM (
        SELECT pubAppId, country, did, SUM(revenue) AS revenue
        FROM lr
        WHERE serverTs IS NOT NULL
        AND y='[(${year})]'
        AND m='[(${month})]'
        AND d='[(${day})]'
        AND revenue > 0
        AND type = 6
        GROUP BY pubAppId, country, did
    )) as a
WHERE max_runk > 10
GROUP BY pubAppId,country;