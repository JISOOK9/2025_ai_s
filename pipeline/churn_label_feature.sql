-- Assigns churn labels based on subscription end date proximity.
-- Parameters:
--   :as_of_date - reference date for feature extraction (YYYY-MM-DD)
--   :n_days     - window length as "+7 day" format
SELECT
    user_id,
    prod_id,
    CASE
        WHEN sbsc_end_dt BETWEEN DATE(:as_of_date)
                             AND DATE(:as_of_date, :n_days)
        THEN 1 ELSE 0
    END AS label
FROM subscriptions;
