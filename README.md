
# PRD: 시계열 기반 구독 해지 시기 예측 & 선제 쿠폰/프로모션 자동화

> **요약**
> 구독허브의 결제, 상품, 상태 데이터와 웹 행동로그(검색, FAQ, 해지페이지 방문)를 통합하여 구독 해지 위험 시점을 예측하고, 위험도가 높은 사용자에게 자동으로 쿠폰이나 프로모션을 제공하는 시스템을 구축한다.
> 모델은 시계열 + 정적 피처 기반 앙상블 구조(LSTM/GRU + GBDT)로 설계하며, 매월 파인튜닝과 경량화를 거쳐 AWS ECS 환경에서 실시간 서빙한다.

---

## 0. 목표 & KPI

- 해지율 상대 10% 감소
- 고위험군 프로모션 전환율 +3pp 상승
- 발송량 30% 절감

**모델 KPI**

- ROC-AUC ≥ 0.78
- PR-AUC ≥ baseline + 30%
- Precision@TopK ≥ 0.35

---

## 1. 범위

**포함(In)**
- 해지 위험 점수 예측 (배치/실시간)
- 행동로그 기반 피처 반영
- 프로모션 추천 로직
- LLM+RAG 메시지 생성
- 모델 학습/파인튜닝/경량화 파이프라인
- Angular UI 연동, 관리자 대시보드

**제외(Out)**
- 결제 게이트웨이 수정
- 외부 제휴 계약

---

## 2. 시스템 아키텍처

```
[Postgres] --ETL--> [Feature Store] --train--> [Model Registry] --deploy--> [ECS ai-service]
[Web/App Logs] --> [Kafka/S3] --ETL--> [Behavior Features] ----/
```

---

## 3. 데이터 모델 & 스키마

### ai_user_metrics
```sql
CREATE TABLE ai_user_metrics (
  user_id UUID,
  prod_id TEXT,
  score NUMERIC(5,4),
  risk_level TEXT,
  model_version TEXT,
  scored_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY(user_id, prod_id)
);
```

### ai_scored_event
```sql
CREATE TABLE ai_scored_event (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID,
  prod_id TEXT,
  event_type TEXT,
  feature_date DATE,
  score NUMERIC(5,4),
  threshold NUMERIC(5,4),
  model_version TEXT,
  explanation JSONB,
  scored_at TIMESTAMPTZ DEFAULT now()
);
```

### ai_action_log
```sql
CREATE TABLE ai_action_log (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID,
  prod_id TEXT,
  action_type TEXT,
  payload JSONB,
  result TEXT,
  related_event_id BIGINT REFERENCES ai_scored_event(id),
  created_at TIMESTAMPTZ DEFAULT now()
);
```

---

## 4. 데이터 파이프라인

**라벨링 로직**
- 해지일(`SBSC_END_DT`) N일 전 데이터: label=1
- 그 외: label=0
- SQL: `churn_label_feature.sql` 사용

**피처**
- 결제(14/30d): fail_cnt, succ_cnt, coupon_amt, refund_amt, avg_amt
- 상품 변경(14/30d): switch_cnt, downgraded
- 행동(14d): cancel_keyword_search, faq_cancel_views, cancel_page_visit

**저장**
- 학습용: S3/Parquet
- 서빙용: Redis 캐시 + Postgres MV

---

## 5. 모델 설계

- GBDT(HistGradientBoosting or LightGBM) + LSTM 앙상블
- 시계열 데이터 분리 학습(Train/Val/Test)
- 월 단위 파인튜닝(Optuna)
- ONNX 변환 + int8 양자화
- AWS ECS Fargate 배포

---

## 6. API 설계

### POST /api/v1/churn/score
Request:
```json
{ "user_id": "uuid", "prod_id": "PROD123" }
```
Response:
```json
{
  "model_version": "v1.2.0",
  "score": 0.83,
  "risk_level": "high"
}
```

### POST /api/v1/churn/score/detail
Request:
```json
{ "user_id": "uuid", "prod_id": "PROD123" }
```
Response:
```json
{
  "model_version": "v1.2.0",
  "score": 0.83,
  "risk_level": "high",
  "top_factors": [
    {"name": "cancel_page_visit_14d", "contribution": 0.21}
  ],
  "recommendation": {"action": "coupon", "params": {"coupon_id": "SAVE10"}}
}
```

### POST /api/v1/promotion/recommend
- score, risk_level 기반 혜택 결정

### POST /api/v1/message/generate
- RAG 기반 개인화 메시지 생성

---

## 7. 프로모션 엔진

- risk_level=high + cancel_page_visit → 쿠폰 발급
- 월 1회 쿠폰 제한
- 관리자 대시보드에서 룰 수정 가능

---

## 8. LLM+RAG

- 코퍼스: FAQ, 약관, 쿠폰 조건
- Embedding: e5-base / ko-sbert
- Vector index: pgvector
- LLM: 7B 한국어 모델 LoRA 파인튜닝
- Guardrails: 길이 제한, 금지어 필터

---

## 9. 프론트엔드

- Angular UI: 구독 상세에 점수, 원인, CTA 표시
- 관리자 페이지: 임계치/룰 조정, A/B 테스트

---

## 10. 보안 및 운영

- PII 암호화, RBAC
- 감사로그(ai_action_log)
- SLO: p95 < 200ms

---

## 11. 테스트 계획

- 단위: API 응답 정확성
- 통합: ETL→모델→API→프로모션 흐름
- 성능: 200rps 처리 가능

---

## 12. 일정

- W1–2: 스키마, ETL 구축
- W3–4: 모델 학습, 검증
- W5–6: API 개발, 프로모션 엔진
- W7–8: UI 통합, QA
- W9: PoC A/B 테스트
- W10–12: 전체 롤아웃
