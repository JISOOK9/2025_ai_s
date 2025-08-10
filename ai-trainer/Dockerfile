
FROM python:3.11-slim
WORKDIR /app
COPY ai-trainer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ai-trainer /app
VOLUME ["/data","/artifacts"]
ENV DB_URL="" MODEL_BUCKET=""
CMD bash -lc "python etl/build_features.py && python train/train_churn.py && python train/export_onnx.py && python registry/push_to_s3.py || true"
