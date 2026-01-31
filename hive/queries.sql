USE clickstream;

-- 1) DAU trend
SELECT dt, dau, events, purchases, purchase_rate
FROM daily_metrics
ORDER BY dt;

-- 2) Top referrers by purchases
SELECT dt, referrer, users_purchase, checkout_to_purchase
FROM funnel_metrics
ORDER BY dt, users_purchase DESC
LIMIT 20;

-- 3) Average session duration + purchase sessions
SELECT dt,
       AVG(session_duration_sec) AS avg_session_sec,
       SUM(made_purchase) AS purchase_sessions,
       COUNT(*) AS total_sessions
FROM sessions
GROUP BY dt
ORDER BY dt;

-- 4) High-activity users (example)
SELECT dt, user_id, COUNT(*) AS events
FROM events_clean
GROUP BY dt, user_id
ORDER BY events DESC
LIMIT 25;
