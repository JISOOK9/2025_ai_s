
import os, joblib, pandas as pd, numpy as np, optuna
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.model_selection import train_test_split
F=['fail_cnt_14d','succ_cnt_14d','coupon_amt_14d','refund_amt_14d','avg_amt_14d','fail_cnt_30d','succ_cnt_30d','coupon_amt_30d','refund_amt_30d','avg_amt_30d','switch_cnt_14d','switch_cnt_30d','downgraded_30d','cancel_keyword_search_14d','faq_cancel_views_14d','cancel_page_visit_14d']
df=pd.read_parquet("/data/features/churn/dt=current.parquet"); X=df[F].fillna(0.0).astype(float).values; y=df["label"].astype(int).values
Xtr,Xte,ytr,yte=train_test_split(X,y,test_size=0.2,shuffle=False); Xtr,Xva,ytr,yva=train_test_split(Xtr,ytr,test_size=0.2,shuffle=False)
def obj(t): 
    clf=HistGradientBoostingClassifier(learning_rate=t.suggest_float("lr",0.01,0.2,log=True),max_iter=t.suggest_int("it",200,800),max_depth=t.suggest_int("md",3,20),l2_regularization=t.suggest_float("l2",0,1),min_samples_leaf=t.suggest_int("msl",10,100),class_weight="balanced")
    clf.fit(Xtr,ytr); from sklearn.metrics import average_precision_score as ap; return ap(yva, clf.predict_proba(Xva)[:,1])
import optuna; s=optuna.create_study(direction="maximize"); s.optimize(obj, n_trials=10)
p=s.best_params; clf=HistGradientBoostingClassifier(learning_rate=p['lr'],max_iter=p['it'],max_depth=p['md'],l2_regularization=p['l2'],min_samples_leaf=p['msl'],class_weight="balanced").fit(Xtr,ytr)
prob=clf.predict_proba(Xte)[:,1]; auc=roc_auc_score(yte,prob) if len(np.unique(yte))>1 else float("nan"); ap=average_precision_score(yte,prob) if len(np.unique(yte))>1 else float("nan")
os.makedirs("/artifacts",exist_ok=True); joblib.dump({"model":clf,"features":F,"metrics":{"auc":auc,"ap":ap}},"/artifacts/model.joblib"); print("Saved model.joblib",auc,ap)
