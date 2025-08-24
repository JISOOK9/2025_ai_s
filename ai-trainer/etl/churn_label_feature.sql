-- =========================================================
--  Churn Features POC: Materialized View + Indexes
--  - 최근 NDAYS만 대상으로 원천을 '일단위 집계 → 14/30d 롤링' 변환
--  - 해지 N일 전 구간 라벨(label=1) 생성
--  - MV + 고유 인덱스(Concurrent Refresh용) + 조회 인덱스
-- =========================================================

-- 1) MV 생성
CREATE MATERIALIZED VIEW IF NOT EXISTS subhub.mv_churn_features_poc AS
WITH
params AS (
  SELECT
    CURRENT_DATE::date AS as_of,
    90  ::int AS NDAYS,         -- 최근 90일만 계산(POC용)
    14  ::int AS WIN14,
    30  ::int AS WIN30,
    14  ::int AS N_BEFORE,      -- 해지 N일 전 라벨 구간
    'SUCCESS'::text AS SUCCESS_CODE,
    ARRAY['DOWN','DOWNGRADE']::text[] AS DOWNGRADE_CODES,
    'CANCELLED'::text AS CANCEL_CODE
),

-- 최근 활동 있는 구독만 대상 축소
target_sbsc AS (
  SELECT DISTINCT sbsc_id
  FROM subhub.tbil_sbsc_stat_l
  WHERE COALESCE(sbsc_st_dt, '1900-01-01') >= (SELECT as_of - (NDAYS+30) * INTERVAL '1 day' FROM params)
     OR COALESCE(sbsc_end_dt, '1900-01-01') >= (SELECT as_of - (NDAYS+30) * INTERVAL '1 day' FROM params)
  UNION
  SELECT DISTINCT sbsc_id
  FROM tbil_sbsc_pmt_hist_l
  WHERE pmt_dt >= (SELECT as_of - (NDAYS+30) * INTERVAL '1 day' FROM params)
),

-- 날짜 spine (최근 NDAYS만)
spine AS (
  SELECT t.sbsc_id, dd::date AS feature_date
  FROM target_sbsc t
  CROSS JOIN LATERAL generate_series(
    (SELECT as_of - NDAYS * INTERVAL '1 day' FROM params),
    (SELECT as_of FROM params),
    '1 day'
  ) AS dd
),

-- 결제: 일단위 사전집계
pmt_daily AS (
  SELECT
    sbsc_id,
    date_trunc('day', pmt_dt)::date AS d,
    COUNT(*) FILTER (WHERE pmt_rslt_cd <> (SELECT SUCCESS_CODE FROM params)) AS fail_cnt,
    COUNT(*) FILTER (WHERE pmt_rslt_cd  = (SELECT SUCCESS_CODE FROM params)) AS succ_cnt,
    SUM(COALESCE(coupn_amt,0)) AS coupon_amt,
    SUM(COALESCE(rfnd_amt,0))  AS refund_amt,
    AVG(NULLIF(tot_amt,0))     AS avg_amt
  FROM tbil_sbsc_pmt_hist_l
  WHERE pmt_dt >= (SELECT as_of - (NDAYS+30) * INTERVAL '1 day' FROM params)
    AND pmt_dt <  (SELECT as_of + INTERVAL '1 day' FROM params)
  GROUP BY sbsc_id, d
),

-- 결제: spine과 결합(결측 0/NULL 보정)
pmt_fill AS (
  SELECT s.sbsc_id, s.feature_date AS d,
         COALESCE(pd.fail_cnt,0)     AS fail_cnt,
         COALESCE(pd.succ_cnt,0)     AS succ_cnt,
         COALESCE(pd.coupon_amt,0)   AS coupon_amt,
         COALESCE(pd.refund_amt,0)   AS refund_amt,
         COALESCE(pd.avg_amt, NULL)  AS avg_amt
  FROM spine s
  LEFT JOIN pmt_daily pd
    ON pd.sbsc_id = s.sbsc_id AND pd.d = s.feature_date
),

-- 결제: 14/30일 롤링(직전일까지)
pmt_roll AS (
  SELECT
    sbsc_id, d AS feature_date,
    SUM(fail_cnt)   OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '14 days' PRECEDING AND '1 day' PRECEDING) AS fail_cnt_14d,
    SUM(succ_cnt)   OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '14 days' PRECEDING AND '1 day' PRECEDING) AS succ_cnt_14d,
    SUM(coupon_amt) OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '14 days' PRECEDING AND '1 day' PRECEDING) AS coupon_amt_14d,
    SUM(refund_amt) OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '14 days' PRECEDING AND '1 day' PRECEDING) AS refund_amt_14d,
    AVG(avg_amt)    OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '14 days' PRECEDING AND '1 day' PRECEDING) AS avg_amt_14d,

    SUM(fail_cnt)   OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '30 days' PRECEDING AND '1 day' PRECEDING) AS fail_cnt_30d,
    SUM(succ_cnt)   OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '30 days' PRECEDING AND '1 day' PRECEDING) AS succ_cnt_30d,
    SUM(coupon_amt) OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '30 days' PRECEDING AND '1 day' PRECEDING) AS coupon_amt_30d,
    SUM(refund_amt) OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '30 days' PRECEDING AND '1 day' PRECEDING) AS refund_amt_30d,
    AVG(avg_amt)    OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '30 days' PRECEDING AND '1 day' PRECEDING) AS avg_amt_30d
  FROM pmt_fill
),

-- 상품 전환: 일단위 사전집계
swt_daily AS (
  SELECT
    sbsc_id,
    date_trunc('day', swtch_prod_dt)::date AS d,
    COUNT(*) AS switch_cnt,
    COUNT(*) FILTER (
      WHERE swtch_prod_type_cd = ANY((SELECT DOWNGRADE_CODES FROM params))
    ) AS downgraded_cnt
  FROM tbil_sbsc_prod_swtch_l
  WHERE swtch_prod_dt >= (SELECT as_of - (NDAYS+30) * INTERVAL '1 day' FROM params)
    AND swtch_prod_dt <  (SELECT as_of + INTERVAL '1 day' FROM params)
  GROUP BY sbsc_id, d
),

-- 상품 전환: spine 결합(결측 보정)
swt_fill AS (
  SELECT s.sbsc_id, s.feature_date AS d,
         COALESCE(sd.switch_cnt,0)     AS switch_cnt,
         COALESCE(sd.downgraded_cnt,0) AS downgraded_cnt
  FROM spine s
  LEFT JOIN swt_daily sd
    ON sd.sbsc_id = s.sbsc_id AND sd.d = s.feature_date
),

-- 상품 전환: 14/30일 롤링
swt_roll AS (
  SELECT
    sbsc_id, d AS feature_date,
    SUM(switch_cnt)     OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '14 days' PRECEDING AND '1 day' PRECEDING) AS switch_cnt_14d,
    SUM(switch_cnt)     OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '30 days' PRECEDING AND '1 day' PRECEDING) AS switch_cnt_30d,
    SUM(downgraded_cnt) OVER (PARTITION BY sbsc_id ORDER BY d
      RANGE BETWEEN '30 days' PRECEDING AND '1 day' PRECEDING) AS downgraded_30d
  FROM swt_fill
),

-- 해지 이벤트 추출
cancel_events AS (
  SELECT sbsc_id, MIN(sbsc_end_dt)::date AS cancel_dt
  FROM subhub.tbil_sbsc_stat_l
  WHERE sbsc_stat_cd = (SELECT CANCEL_CODE FROM params)
  GROUP BY sbsc_id
),

-- 라벨링: 해지 N일 전 구간 = 1
label AS (
  SELECT
    s.sbsc_id, s.feature_date,
    CASE WHEN c.cancel_dt IS NOT NULL
          AND s.feature_date BETWEEN c.cancel_dt - (SELECT N_BEFORE FROM params) * INTERVAL '1 day'
                                 AND c.cancel_dt
         THEN 1 ELSE 0 END AS label
  FROM spine s
  LEFT JOIN cancel_events c USING (sbsc_id)
),

-- 정적 식별자(리포트용)
sbsc_identity AS (
  SELECT sbsc_id, MAX(buyer_guid) AS buyer_guid
  FROM subhub.tbil_sbsc_stat_l
  GROUP BY sbsc_id
)

SELECT
  s.sbsc_id,
  i.buyer_guid,
  s.feature_date,
  l.label,
  -- 결제 피처
  pr.fail_cnt_14d, pr.succ_cnt_14d, pr.coupon_amt_14d, pr.refund_amt_14d, pr.avg_amt_14d,
  pr.fail_cnt_30d, pr.succ_cnt_30d, pr.coupon_amt_30d, pr.refund_amt_30d, pr.avg_amt_30d,
  -- 전환 피처
  sr.switch_cnt_14d, sr.switch_cnt_30d, sr.downgraded_30d
FROM spine s
LEFT JOIN pmt_roll pr USING (sbsc_id, feature_date)
LEFT JOIN swt_roll sr USING (sbsc_id, feature_date)
LEFT JOIN label    l  USING (sbsc_id, feature_date)
LEFT JOIN sbsc_identity i USING (sbsc_id);

-- 2) MV 인덱스(Concurrent Refresh 위해 유니크 인덱스 필수)
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_churn_features_poc
  ON subhub.mv_churn_features_poc (sbsc_id, feature_date);

-- 조회 최적화(세그먼트/기간 조회)
CREATE INDEX IF NOT EXISTS ix_mv_churn_features_poc_date
  ON subhub.mv_churn_features_poc (feature_date);

-- (선택) feature 조회 자주 쓰면 buyer_guid 포함
CREATE INDEX IF NOT EXISTS ix_mv_churn_features_poc_buyer
  ON subhub.mv_churn_features_poc (buyer_guid, feature_date);

-- 3) 통계 업데이트(옵션)
ANALYZE subhub.mv_churn_features_poc;

-- 4) 이후 일일 갱신 시 사용 예:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY subhub.mv_churn_features_poc;
-- (CONCURRENTLY 사용하려면 위 '유니크 인덱스'가 반드시 있어야 합니다)
