-- 구독 해지 예측 시스템 데이터베이스 스키마

-- 사용자 메트릭 테이블 (실시간 점수 저장)
CREATE TABLE IF NOT EXISTS ai_user_metrics (
    user_id UUID NOT NULL,
    prod_id TEXT NOT NULL,
    score NUMERIC(5,4) NOT NULL CHECK (score >= 0 AND score <= 1),
    risk_level TEXT NOT NULL CHECK (risk_level IN ('low', 'medium', 'high')),
    model_version TEXT NOT NULL,
    scored_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY(user_id, prod_id)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_ai_user_metrics_risk_level ON ai_user_metrics(risk_level);
CREATE INDEX IF NOT EXISTS idx_ai_user_metrics_scored_at ON ai_user_metrics(scored_at);
CREATE INDEX IF NOT EXISTS idx_ai_user_metrics_model_version ON ai_user_metrics(model_version);

-- 점수 이벤트 테이블 (점수 계산 이력)
CREATE TABLE IF NOT EXISTS ai_scored_event (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    prod_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('batch_scoring', 'realtime_scoring', 'manual_scoring')),
    feature_date DATE NOT NULL,
    score NUMERIC(5,4) NOT NULL CHECK (score >= 0 AND score <= 1),
    threshold NUMERIC(5,4) NOT NULL,
    model_version TEXT NOT NULL,
    explanation JSONB, -- 피처 중요도 및 설명
    scored_at TIMESTAMPTZ DEFAULT now()
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_ai_scored_event_user_prod ON ai_scored_event(user_id, prod_id);
CREATE INDEX IF NOT EXISTS idx_ai_scored_event_feature_date ON ai_scored_event(feature_date);
CREATE INDEX IF NOT EXISTS idx_ai_scored_event_scored_at ON ai_scored_event(scored_at);

-- 액션 로그 테이블 (프로모션 발송 등 액션 이력)
CREATE TABLE IF NOT EXISTS ai_action_log (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    prod_id TEXT NOT NULL,
    action_type TEXT NOT NULL CHECK (action_type IN ('coupon_issued', 'promotion_sent', 'message_sent', 'manual_intervention')),
    payload JSONB NOT NULL, -- 액션 상세 정보
    result TEXT NOT NULL CHECK (result IN ('success', 'failed', 'pending')),
    related_event_id BIGINT REFERENCES ai_scored_event(id),
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_ai_action_log_user_prod ON ai_action_log(user_id, prod_id);
CREATE INDEX IF NOT EXISTS idx_ai_action_log_action_type ON ai_action_log(action_type);
CREATE INDEX IF NOT EXISTS idx_ai_action_log_created_at ON ai_action_log(created_at);
CREATE INDEX IF NOT EXISTS idx_ai_action_log_related_event ON ai_action_log(related_event_id);

-- 피처 스토어 테이블 (학습 및 서빙용 피처)
CREATE TABLE IF NOT EXISTS ai_feature_store (
    user_id UUID NOT NULL,
    prod_id TEXT NOT NULL,
    feature_date DATE NOT NULL,
    
    -- 결제 관련 피처
    payment_fail_cnt_14d INTEGER DEFAULT 0,
    payment_fail_cnt_30d INTEGER DEFAULT 0,
    payment_succ_cnt_14d INTEGER DEFAULT 0,
    payment_succ_cnt_30d INTEGER DEFAULT 0,
    coupon_amt_14d NUMERIC(10,2) DEFAULT 0,
    coupon_amt_30d NUMERIC(10,2) DEFAULT 0,
    refund_amt_14d NUMERIC(10,2) DEFAULT 0,
    refund_amt_30d NUMERIC(10,2) DEFAULT 0,
    avg_payment_amt_14d NUMERIC(10,2) DEFAULT 0,
    avg_payment_amt_30d NUMERIC(10,2) DEFAULT 0,
    
    -- 상품 변경 관련 피처
    product_switch_cnt_14d INTEGER DEFAULT 0,
    product_switch_cnt_30d INTEGER DEFAULT 0,
    downgrade_cnt_14d INTEGER DEFAULT 0,
    downgrade_cnt_30d INTEGER DEFAULT 0,
    
    -- 행동 로그 피처
    cancel_keyword_search_14d INTEGER DEFAULT 0,
    faq_cancel_views_14d INTEGER DEFAULT 0,
    cancel_page_visit_14d INTEGER DEFAULT 0,
    support_contact_14d INTEGER DEFAULT 0,
    
    -- 메타 정보
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    
    PRIMARY KEY(user_id, prod_id, feature_date)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_ai_feature_store_feature_date ON ai_feature_store(feature_date);
CREATE INDEX IF NOT EXISTS idx_ai_feature_store_user_prod ON ai_feature_store(user_id, prod_id);

-- 모델 메타데이터 테이블
CREATE TABLE IF NOT EXISTS ai_model_metadata (
    id BIGSERIAL PRIMARY KEY,
    model_version TEXT NOT NULL UNIQUE,
    model_type TEXT NOT NULL CHECK (model_type IN ('gbdt', 'lstm', 'ensemble')),
    model_path TEXT NOT NULL, -- S3 경로 또는 로컬 경로
    performance_metrics JSONB, -- ROC-AUC, PR-AUC 등
    feature_importance JSONB, -- 피처 중요도
    training_config JSONB, -- 학습 설정
    is_active BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT now(),
    deployed_at TIMESTAMPTZ
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_ai_model_metadata_version ON ai_model_metadata(model_version);
CREATE INDEX IF NOT EXISTS idx_ai_model_metadata_active ON ai_model_metadata(is_active);

-- 배치 작업 로그 테이블
CREATE TABLE IF NOT EXISTS ai_batch_job_log (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL CHECK (job_type IN ('feature_extraction', 'model_training', 'batch_scoring', 'model_deployment')),
    job_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    duration_seconds INTEGER,
    processed_records INTEGER DEFAULT 0,
    error_message TEXT,
    job_config JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_ai_batch_job_log_job_type ON ai_batch_job_log(job_type);
CREATE INDEX IF NOT EXISTS idx_ai_batch_job_log_status ON ai_batch_job_log(status);
CREATE INDEX IF NOT EXISTS idx_ai_batch_job_log_start_time ON ai_batch_job_log(start_time);

-- 업데이트 트리거 함수
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 업데이트 트리거 생성
CREATE TRIGGER update_ai_user_metrics_updated_at 
    BEFORE UPDATE ON ai_user_metrics 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ai_action_log_updated_at 
    BEFORE UPDATE ON ai_action_log 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ai_feature_store_updated_at 
    BEFORE UPDATE ON ai_feature_store 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 파티셔닝을 위한 함수 (월별 파티션)
CREATE OR REPLACE FUNCTION create_monthly_partition(table_name TEXT, start_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    end_date DATE;
BEGIN
    partition_name := table_name || '_' || to_char(start_date, 'YYYY_MM');
    end_date := start_date + INTERVAL '1 month';
    
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I 
                    FOR VALUES FROM (%L) TO (%L)',
                   partition_name, table_name, start_date, end_date);
END;
$$ LANGUAGE plpgsql;

-- 뷰 생성: 최신 사용자 메트릭
CREATE OR REPLACE VIEW v_latest_user_metrics AS
SELECT 
    user_id,
    prod_id,
    score,
    risk_level,
    model_version,
    scored_at
FROM ai_user_metrics
WHERE scored_at >= now() - INTERVAL '7 days';

-- 뷰 생성: 고위험 사용자 목록
CREATE OR REPLACE VIEW v_high_risk_users AS
SELECT 
    user_id,
    prod_id,
    score,
    scored_at,
    model_version
FROM ai_user_metrics
WHERE risk_level = 'high'
  AND scored_at >= now() - INTERVAL '1 day';
