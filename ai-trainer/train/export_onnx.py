
import joblib
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
b=joblib.load("/artifacts/model.joblib"); m=b["model"]; n=len(b["features"])
onnx=convert_sklearn(m, initial_types=[("input", FloatTensorType([None,n]))])
open("/artifacts/model.onnx","wb").write(onnx.SerializeToString()); print("Saved model.onnx")
