WITH events AS (
    SELECT
        *
        -- time based dimensions
        , CASE
            WHEN strftime('%H', timestamp) >= '00' AND strftime('%H', timestamp) < '06' THEN 0
            WHEN strftime('%H', timestamp) >= '06' AND strftime('%H', timestamp) < '12' THEN 1
            WHEN strftime('%H', timestamp) >= '12' AND strftime('%H', timestamp) < '18' THEN 2
            WHEN strftime('%H', timestamp) >= '18' AND strftime('%H', timestamp) < '24' THEN 3
        END AS hour_range
        , strftime('%w', timestamp) AS week_day
        , SUM(CASE WHEN event = 'view' THEN 1 ELSE 0 END) OVER (PARTITION BY visitorid, strftime('%Y-%m-%d %H:%M', timestamp)) AS views_per_minute
        -- consecutive events on the same itemid
        , CASE
            WHEN itemid = LEAD(itemid, 1) OVER (PARTITION BY visitorid ORDER BY timestamp) THEN 1
            ELSE 0 END AS repitem
        , CASE
            WHEN
                itemid = LEAD(itemid, 1) OVER (PARTITION BY visitorid ORDER BY timestamp)
                AND event = 'view'
                THEN 1
            ELSE 0 END AS repitem_fview
        , CASE
            WHEN
                itemid = LEAD(itemid, 1) OVER (PARTITION BY visitorid ORDER BY timestamp)
                AND event = 'view'
                AND LEAD(event, 1) OVER (PARTITION BY visitorid ORDER BY timestamp) = 'view'
                THEN 1
            ELSE 0 END AS repitem_bview
        -- funnel flags
        , CASE
            WHEN
                event = 'view'
                AND LEAD(event, 1) OVER (PARTITION BY visitorid ORDER BY timestamp) = 'addtocart'
                AND LEAD(event, 2) OVER (PARTITION BY visitorid ORDER BY timestamp) = 'transaction'
            THEN 1
            ELSE 0 END AS funnel_flag
    FROM df_events
),
visitor_session AS (
    SELECT
        visitorid
        , MIN(num_views) AS min_vipse
        , AVG(num_views) AS avg_vipse
        , MAX(num_views) AS max_vipse
        , MIN(num_acart) AS min_acpse
        , AVG(num_acart) AS avg_acpse
        , MAX(num_acart) AS max_acpse
        , MIN(num_purch) AS min_prpse
        , AVG(num_purch) AS avg_prpse
        , MAX(num_purch) AS max_prpse
    FROM (
        SELECT
            visitorid
            , session_id
            , SUM(CASE WHEN event = 'view' THEN 1 ELSE 0 END) AS num_views
            , SUM(CASE WHEN event = 'addtocart' THEN 1 ELSE 0 END) AS num_acart
            , SUM(CASE WHEN event = 'transaction' THEN 1 ELSE 0 END) AS num_purch
        FROM events
        GROUP BY visitorid, session_id
    )
    GROUP BY visitorid
)

SELECT
    a.visitorid
    , SUM(CASE WHEN event = 'view' THEN 1 ELSE 0 END) AS num_views
    , SUM(CASE WHEN event = 'addtocart' THEN 1 ELSE 0 END) AS num_acart
    , SUM(CASE WHEN event = 'transaction' THEN 1 ELSE 0 END) AS num_purch
    , COUNT(1) AS num_evnts
    , COUNT(DISTINCT CASE WHEN transactionid != -1 THEN transactionid ELSE NULL END) AS num_txs
    , SUM(CASE WHEN hour_range =0 THEN 1 ELSE 0 END) AS num_ev06h
    , SUM(CASE WHEN hour_range =1 THEN 1 ELSE 0 END) AS num_ev12h
    , SUM(CASE WHEN hour_range =2 THEN 1 ELSE 0 END) AS num_ev18h
    , SUM(CASE WHEN hour_range =3 THEN 1 ELSE 0 END) AS num_ev00h
    , SUM(CASE WHEN week_day =0 THEN 1 ELSE 0 END) AS num_evsun
    , SUM(CASE WHEN week_day =1 THEN 1 ELSE 0 END) AS num_evmon
    , SUM(CASE WHEN week_day =2 THEN 1 ELSE 0 END) AS num_evtue
    , SUM(CASE WHEN week_day =3 THEN 1 ELSE 0 END) AS num_evwed
    , SUM(CASE WHEN week_day =4 THEN 1 ELSE 0 END) AS num_evthu
    , SUM(CASE WHEN week_day =5 THEN 1 ELSE 0 END) AS num_evfri
    , SUM(CASE WHEN week_day =6 THEN 1 ELSE 0 END) AS num_evsat
    , SUM(repitem) AS num_rep
    , SUM(repitem_fview) AS num_repfv
    , SUM(repitem_bview) AS num_repbv
    , SUM(funnel_flag) AS num_funnl
    , MAX(views_per_minute) AS max_vpmin
    , b.min_vipse
    , b.avg_vipse
    , b.max_vipse
    , b.min_acpse
    , b.avg_acpse
    , b.max_acpse
    , b.min_prpse
    , b.avg_prpse
    , b.max_prpse
FROM events AS a
    JOIN visitor_session AS b ON a.visitorid = b.visitorid
GROUP BY a.visitorid
HAVING
    NOT (num_views <=1 AND num_acart =0 AND num_txs =0)