
# README: Upgraded Training with Validation Reports

This script (`train_churn_with_report.py`) trains the churn model **and** produces a validation report with:
- ROC and PR curves (PNG)
- Confusion matrices at thresholds 0.30 / 0.50 / 0.70 / 0.80 (PNG)
- Metrics JSON + Markdown report

## Requirements
- Features parquet at: `/data/features/churn/dt=current.parquet`
- Columns: the feature set + `label` (0/1)
- Packages: `scikit-learn`, `optuna`, `pandas`, `numpy`, `matplotlib`, `joblib`

## Run
```bash
python train_churn_with_report.py
```

## Outputs (in `/artifacts`)
- `model.joblib`
- `metrics.json`
- `roc_curve.png`
- `pr_curve.png`
- `confusion_matrix_threshold_*.png`
- `report.md`
