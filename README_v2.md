
# Subscription Churn Scaffold (Split)
- ai-trainer/: ETL + Training + Export (Python)
- ai-service/: Real-time scoring API (Spring Boot WebFlux)
- promotion-engine/: Rules

## Quick Start
1) Fill full SQL in ai-trainer/etl/churn_label_feature.sql
2) Run trainer with DB_URL to generate /artifacts/model.onnx
3) Build ai-service and run with MODEL_PATH pointing to model.onnx
