-- 구독 해지 라벨링 및 피처 추출 쿼리
-- 해지일 N일 전 데이터를 label=1로, 그 외는 label=0으로 설정

WITH churn_labels AS (
    -- 해지 라벨 생성: 해지일 14일 전 데이터를 양성 라벨로 설정
    SELECT 
        s.user_id,
        s.prod_id,
        s.sbsc_start_dt,
        s.sbsc_end_dt,
        CASE 
            WHEN s.sbsc_end_dt IS NOT NULL 
                 AND s.sbsc_end_dt <= CURRENT_DATE 
                 AND s.sbsc_end_dt > s.sbsc_start_dt
            THEN 1 
            ELSE 0 
        END as churn_label,
        -- 피처 추출 기준일: 해지일 14일 전 또는 현재일
        CASE 
            WHEN s.sbsc_end_dt IS NOT NULL 
            THEN s.sbsc_end_dt - INTERVAL '14 days'
            ELSE CURRENT_DATE
        END as feature_date
    FROM subscription s
    WHERE s.sbsc_start_dt IS NOT NULL
      AND s.sbsc_start_dt <= CURRENT_DATE - INTERVAL '30 days' -- 최소 30일 이상 구독한 사용자만
),

payment_features AS (
    -- 결제 관련 피처 추출
    SELECT 
        cl.user_id,
        cl.prod_id,
        cl.feature_date,
        
        -- 14일 윈도우 결제 피처
        COUNT(CASE WHEN p.payment_dt >= cl.feature_date - INTERVAL '14 days' 
                        AND p.payment_dt < cl.feature_date 
                        AND p.payment_status = 'FAILED' 
                   THEN 1 END) as payment_fail_cnt_14d,
                   
        COUNT(CASE WHEN p.payment_dt >= cl.feature_date - INTERVAL '14 days' 
                        AND p.payment_dt < cl.feature_date 
                        AND p.payment_status = 'SUCCESS' 
                   THEN 1 END) as payment_succ_cnt_14d,
                   
        COALESCE(SUM(CASE WHEN p.payment_dt >= cl.feature_date - INTERVAL '14 days' 
                              AND p.payment_dt < cl.feature_date 
                              AND p.coupon_amt > 0 
                         THEN p.coupon_amt END), 0) as coupon_amt_14d,
                         
        COALESCE(SUM(CASE WHEN p.payment_dt >= cl.feature_date - INTERVAL '14 days' 
                              AND p.payment_dt < cl.feature_date 
                              AND p.refund_amt > 0 
                         THEN p.refund_amt END), 0) as refund_amt_14d,
                         
        COALESCE(AVG(CASE WHEN p.payment_dt >= cl.feature_date - INTERVAL '14 days' 
                              AND p.payment_dt < cl.feature_date 
                              AND p.payment_status = 'SUCCESS' 
                         THEN p.payment_amt END), 0) as avg_payment_amt_14d,
        
        -- 30일 윈도우 결제 피처
        COUNT(CASE WHEN p.payment_dt >= cl.feature_date - INTERVAL '30 days' 
                        AND p.payment_dt < cl.feature_date 
                        AND p.payment_status = 'FAILED' 
                   THEN 1 END) as payment_fail_cnt_30d,
                   
        COUNT(CASE WHEN p.payment_dt >= cl.feature_date - INTERVAL '30 days' 
                        AND p.payment_dt < cl.feature_date 
                        AND p.payment_status = 'SUCCESS' 
                   THEN 1 END) as payment_succ_cnt_30d,
                   
        COALESCE(SUM(CASE WHEN p.payment_dt >= cl.feature_date - INTERVAL '30 days' 
                              AND p.payment_dt < cl.feature_date 
                              AND p.coupon_amt > 0 
                         THEN p.coupon_amt END), 0) as coupon_amt_30d,
                         
        COALESCE(SUM(CASE WHEN p.payment_dt >= cl.feature_date - INTERVAL '30 days' 
                              AND p.payment_dt < cl.feature_date 
                              AND p.refund_amt > 0 
                         THEN p.refund_amt END), 0) as refund_amt_30d,
                         
        COALESCE(AVG(CASE WHEN p.payment_dt >= cl.feature_date - INTERVAL '30 days' 
                              AND p.payment_dt < cl.feature_date 
                              AND p.payment_status = 'SUCCESS' 
                         THEN p.payment_amt END), 0) as avg_payment_amt_30d
                         
    FROM churn_labels cl
    LEFT JOIN payment p ON cl.user_id = p.user_id 
                        AND cl.prod_id = p.prod_id
                        AND p.payment_dt < cl.feature_date
                        AND p.payment_dt >= cl.feature_date - INTERVAL '30 days'
    GROUP BY cl.user_id, cl.prod_id, cl.feature_date
),

product_features AS (
    -- 상품 변경 관련 피처 추출
    SELECT 
        cl.user_id,
        cl.prod_id,
        cl.feature_date,
        
        -- 14일 윈도우 상품 변경 피처
        COUNT(CASE WHEN pc.change_dt >= cl.feature_date - INTERVAL '14 days' 
                        AND pc.change_dt < cl.feature_date 
                   THEN 1 END) as product_switch_cnt_14d,
                   
        COUNT(CASE WHEN pc.change_dt >= cl.feature_date - INTERVAL '14 days' 
                        AND pc.change_dt < cl.feature_date 
                        AND pc.change_type = 'DOWNGRADE'
                   THEN 1 END) as downgrade_cnt_14d,
        
        -- 30일 윈도우 상품 변경 피처
        COUNT(CASE WHEN pc.change_dt >= cl.feature_date - INTERVAL '30 days' 
                        AND pc.change_dt < cl.feature_date 
                   THEN 1 END) as product_switch_cnt_30d,
                   
        COUNT(CASE WHEN pc.change_dt >= cl.feature_date - INTERVAL '30 days' 
                        AND pc.change_dt < cl.feature_date 
                        AND pc.change_type = 'DOWNGRADE'
                   THEN 1 END) as downgrade_cnt_30d
                   
    FROM churn_labels cl
    LEFT JOIN product_change pc ON cl.user_id = pc.user_id 
                                AND cl.prod_id = pc.prod_id
                                AND pc.change_dt < cl.feature_date
                                AND pc.change_dt >= cl.feature_date - INTERVAL '30 days'
    GROUP BY cl.user_id, cl.prod_id, cl.feature_date
),

behavior_features AS (
    -- 행동 로그 피처 추출 (14일 윈도우만 사용)
    SELECT 
        cl.user_id,
        cl.prod_id,
        cl.feature_date,
        
        -- 해지 관련 검색 키워드
        COUNT(CASE WHEN bl.event_dt >= cl.feature_date - INTERVAL '14 days' 
                        AND bl.event_dt < cl.feature_date 
                        AND bl.event_type = 'SEARCH'
                        AND (bl.event_data->>'keyword' ILIKE '%해지%' 
                             OR bl.event_data->>'keyword' ILIKE '%취소%'
                             OR bl.event_data->>'keyword' ILIKE '%중단%')
                   THEN 1 END) as cancel_keyword_search_14d,
                   
        -- FAQ 해지 관련 페이지 조회
        COUNT(CASE WHEN bl.event_dt >= cl.feature_date - INTERVAL '14 days' 
                        AND bl.event_dt < cl.feature_date 
                        AND bl.event_type = 'PAGE_VIEW'
                        AND (bl.event_data->>'page_url' ILIKE '%faq%' 
                             AND (bl.event_data->>'page_url' ILIKE '%해지%'
                                  OR bl.event_data->>'page_url' ILIKE '%cancel%'))
                   THEN 1 END) as faq_cancel_views_14d,
                   
        -- 해지 페이지 직접 방문
        COUNT(CASE WHEN bl.event_dt >= cl.feature_date - INTERVAL '14 days' 
                        AND bl.event_dt < cl.feature_date 
                        AND bl.event_type = 'PAGE_VIEW'
                        AND (bl.event_data->>'page_url' ILIKE '%cancel%'
                             OR bl.event_data->>'page_url' ILIKE '%unsubscribe%')
                   THEN 1 END) as cancel_page_visit_14d,
                   
        -- 고객지원 문의
        COUNT(CASE WHEN bl.event_dt >= cl.feature_date - INTERVAL '14 days' 
                        AND bl.event_dt < cl.feature_date 
                        AND bl.event_type = 'SUPPORT_CONTACT'
                   THEN 1 END) as support_contact_14d
                   
    FROM churn_labels cl
    LEFT JOIN behavior_log bl ON cl.user_id = bl.user_id
                               AND bl.event_dt < cl.feature_date
                               AND bl.event_dt >= cl.feature_date - INTERVAL '14 days'
    GROUP BY cl.user_id, cl.prod_id, cl.feature_date
)

-- 최종 피처 테이블 생성
SELECT 
    cl.user_id,
    cl.prod_id,
    cl.feature_date,
    cl.churn_label,
    cl.sbsc_start_dt,
    cl.sbsc_end_dt,
    
    -- 결제 피처
    COALESCE(pf.payment_fail_cnt_14d, 0) as payment_fail_cnt_14d,
    COALESCE(pf.payment_fail_cnt_30d, 0) as payment_fail_cnt_30d,
    COALESCE(pf.payment_succ_cnt_14d, 0) as payment_succ_cnt_14d,
    COALESCE(pf.payment_succ_cnt_30d, 0) as payment_succ_cnt_30d,
    COALESCE(pf.coupon_amt_14d, 0) as coupon_amt_14d,
    COALESCE(pf.coupon_amt_30d, 0) as coupon_amt_30d,
    COALESCE(pf.refund_amt_14d, 0) as refund_amt_14d,
    COALESCE(pf.refund_amt_30d, 0) as refund_amt_30d,
    COALESCE(pf.avg_payment_amt_14d, 0) as avg_payment_amt_14d,
    COALESCE(pf.avg_payment_amt_30d, 0) as avg_payment_amt_30d,
    
    -- 상품 변경 피처
    COALESCE(prf.product_switch_cnt_14d, 0) as product_switch_cnt_14d,
    COALESCE(prf.product_switch_cnt_30d, 0) as product_switch_cnt_30d,
    COALESCE(prf.downgrade_cnt_14d, 0) as downgrade_cnt_14d,
    COALESCE(prf.downgrade_cnt_30d, 0) as downgrade_cnt_30d,
    
    -- 행동 로그 피처
    COALESCE(bf.cancel_keyword_search_14d, 0) as cancel_keyword_search_14d,
    COALESCE(bf.faq_cancel_views_14d, 0) as faq_cancel_views_14d,
    COALESCE(bf.cancel_page_visit_14d, 0) as cancel_page_visit_14d,
    COALESCE(bf.support_contact_14d, 0) as support_contact_14d,
    
    -- 추가 파생 피처
    CASE WHEN COALESCE(pf.payment_succ_cnt_30d, 0) > 0 
         THEN COALESCE(pf.payment_fail_cnt_30d, 0)::FLOAT / pf.payment_succ_cnt_30d 
         ELSE 0 END as payment_failure_rate_30d,
         
    EXTRACT(DAYS FROM cl.feature_date - cl.sbsc_start_dt) as subscription_days,
    
    CASE WHEN EXTRACT(DOW FROM cl.feature_date) IN (0, 6) THEN 1 ELSE 0 END as is_weekend,
    EXTRACT(MONTH FROM cl.feature_date) as feature_month,
    
    CURRENT_TIMESTAMP as created_at

FROM churn_labels cl
LEFT JOIN payment_features pf ON cl.user_id = pf.user_id 
                              AND cl.prod_id = pf.prod_id 
                              AND cl.feature_date = pf.feature_date
LEFT JOIN product_features prf ON cl.user_id = prf.user_id 
                                AND cl.prod_id = prf.prod_id 
                                AND cl.feature_date = prf.feature_date
LEFT JOIN behavior_features bf ON cl.user_id = bf.user_id 
                                AND cl.prod_id = bf.prod_id 
                                AND cl.feature_date = bf.feature_date
ORDER BY cl.user_id, cl.prod_id, cl.feature_date;
