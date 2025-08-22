
WITH params AS (
    SELECT 
        14::int AS N_DAYS_BEFORE_CANCEL,
        14::int AS WIN_14D,
        30::int AS WIN_30D
),
cancel_events AS (
    SELECT 
        s.sbsc_id,
        MIN(s.stat_end_dt) AS cancel_dt
    FROM TBIL_SBSC_STAT_L s
    WHERE s.sbsc_stat_cd = 'CANCELLED'
    GROUP BY s.sbsc_id
),
base AS (
    SELECT 
        p.sbsc_id,
        d.feature_date,
        CASE 
            WHEN c.cancel_dt IS NOT NULL 
             AND d.feature_date BETWEEN c.cancel_dt - (SELECT N_DAYS_BEFORE_CANCEL FROM params) * INTERVAL '1 day'
                                     AND c.cancel_dt
            THEN 1 ELSE 0
        END AS label
    FROM (
        SELECT DISTINCT sbsc_id FROM TBIL_SBSC_STAT_L
    ) p
    CROSS JOIN LATERAL (
        SELECT generate_series(
            CURRENT_DATE - INTERVAL '365 day',
            CURRENT_DATE,
            INTERVAL '1 day'
        )::date AS feature_date
    ) d
    LEFT JOIN cancel_events c
      ON c.sbsc_id = p.sbsc_id
),
pmt_features AS (
    SELECT 
        sbsc_id,
        feature_date,
        COUNT(*) FILTER (WHERE pmt_rslt_cd != 'SUCCESS'
                         AND pmt_req_dt >= feature_date - (SELECT WIN_14D FROM params) * INTERVAL '1 day'
                         AND pmt_req_dt < feature_date) AS fail_cnt_14d,
        COUNT(*) FILTER (WHERE pmt_rslt_cd = 'SUCCESS'
                         AND pmt_req_dt >= feature_date - (SELECT WIN_14D FROM params) * INTERVAL '1 day'
                         AND pmt_req_dt < feature_date) AS succ_cnt_14d,
        SUM(pmt_amt) FILTER (WHERE coupon_use_yn = 'Y'
                         AND pmt_req_dt >= feature_date - (SELECT WIN_14D FROM params) * INTERVAL '1 day'
                         AND pmt_req_dt < feature_date) AS coupon_amt_14d,
        SUM(refund_amt) FILTER (WHERE pmt_req_dt >= feature_date - (SELECT WIN_14D FROM params) * INTERVAL '1 day'
                         AND pmt_req_dt < feature_date) AS refund_amt_14d,
        AVG(pmt_amt) FILTER (WHERE pmt_req_dt >= feature_date - (SELECT WIN_14D FROM params) * INTERVAL '1 day'
                         AND pmt_req_dt < feature_date) AS avg_amt_14d,

        COUNT(*) FILTER (WHERE pmt_rslt_cd != 'SUCCESS'
                         AND pmt_req_dt >= feature_date - (SELECT WIN_30D FROM params) * INTERVAL '1 day'
                         AND pmt_req_dt < feature_date) AS fail_cnt_30d,
        COUNT(*) FILTER (WHERE pmt_rslt_cd = 'SUCCESS'
                         AND pmt_req_dt >= feature_date - (SELECT WIN_30D FROM params) * INTERVAL '1 day'
                         AND pmt_req_dt < feature_date) AS succ_cnt_30d,
        SUM(pmt_amt) FILTER (WHERE coupon_use_yn = 'Y'
                         AND pmt_req_dt >= feature_date - (SELECT WIN_30D FROM params) * INTERVAL '1 day'
                         AND pmt_req_dt < feature_date) AS coupon_amt_30d,
        SUM(refund_amt) FILTER (WHERE pmt_req_dt >= feature_date - (SELECT WIN_30D FROM params) * INTERVAL '1 day'
                         AND pmt_req_dt < feature_date) AS refund_amt_30d,
        AVG(pmt_amt) FILTER (WHERE pmt_req_dt >= feature_date - (SELECT WIN_30D FROM params) * INTERVAL '1 day'
                         AND pmt_req_dt < feature_date) AS avg_amt_30d
    FROM base b
    LEFT JOIN TBIL_SBSC_PMT_HIST_L p
      ON p.sbsc_id = b.sbsc_id
    GROUP BY sbsc_id, feature_date
),
switch_features AS (
    SELECT 
        sbsc_id,
        feature_date,
        COUNT(*) FILTER (WHERE swtch_req_dt >= feature_date - (SELECT WIN_14D FROM params) * INTERVAL '1 day'
                         AND swtch_req_dt < feature_date) AS switch_cnt_14d,
        COUNT(*) FILTER (WHERE swtch_req_dt >= feature_date - (SELECT WIN_30D FROM params) * INTERVAL '1 day'
                         AND swtch_req_dt < feature_date) AS switch_cnt_30d,
        COUNT(*) FILTER (WHERE downgrade_yn = 'Y'
                         AND swtch_req_dt >= feature_date - (SELECT WIN_30D FROM params) * INTERVAL '1 day'
                         AND swtch_req_dt < feature_date) AS downgraded_30d
    FROM base b
    LEFT JOIN TBIL_SBSC_PROD_SWTCH_L s
      ON s.sbsc_id = b.sbsc_id
    GROUP BY sbsc_id, feature_date
),
behavior_features AS (
    SELECT 
        sbsc_id,
        feature_date,
        COUNT(*) FILTER (WHERE event_type = 'SEARCH' AND keyword ILIKE '%해지%'
                         AND event_dt >= feature_date - (SELECT WIN_14D FROM params) * INTERVAL '1 day'
                         AND event_dt < feature_date) AS cancel_keyword_search_14d,
        COUNT(*) FILTER (WHERE event_type = 'FAQ_VIEW' AND faq_id IN (SELECT id FROM faq WHERE category='CANCEL')
                         AND event_dt >= feature_date - (SELECT WIN_14D FROM params) * INTERVAL '1 day'
                         AND event_dt < feature_date) AS faq_cancel_views_14d,
        COUNT(*) FILTER (WHERE event_type = 'PAGE_VIEW' AND page_url ILIKE '%cancel%'
                         AND event_dt >= feature_date - (SELECT WIN_14D FROM params) * INTERVAL '1 day'
                         AND event_dt < feature_date) AS cancel_page_visit_14d
    FROM base b
    LEFT JOIN user_behavior_log l
      ON l.sbsc_id = b.sbsc_id
    GROUP BY sbsc_id, feature_date
)
SELECT 
    b.user_id,
    b.sbsc_id,
    b.feature_date,
    b.label,
    p.* EXCEPT(user_id, sbsc_id, feature_date),
    s.* EXCEPT(user_id, sbsc_id, feature_date),
    h.* EXCEPT(user_id, sbsc_id, feature_date)
FROM base b
LEFT JOIN pmt_features p USING (user_id, sbsc_id, feature_date)
LEFT JOIN switch_features s USING (user_id, sbsc_id, feature_date)
LEFT JOIN behavior_features h USING (user_id, sbsc_id, feature_date);
