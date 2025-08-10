
import os,boto3,datetime; bucket=os.getenv("MODEL_BUCKET"); ver=os.getenv("MODEL_VERSION",datetime.datetime.utcnow().strftime("churn_v%Y%m%d")); s3=boto3.client("s3")
for f in ["model.joblib","model.onnx"]:
    p=f"/artifacts/{f}"
    if os.path.exists(p): s3.upload_file(p,bucket,f"models/{ver}/{f}"); print("uploaded",f)
