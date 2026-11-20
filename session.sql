CREATE TEMP TABLE session_events AS
WITH sequenced AS (
    SELECT
        canonical_id,
        ts_epoch_ms,
        event_type,
        product_id,
        price_cents,
        lag(ts_epoch_ms) OVER (PARTITION BY canonical_id ORDER BY ts_epoch_ms) AS previous_ts
    FROM events
), assigned AS (
    SELECT
        canonical_id,
        event_type,
        product_id,
        price_cents,
        -- RANGE includes every peer at this timestamp in the new session.
        max(CASE WHEN previous_ts IS NULL OR ts_epoch_ms - previous_ts > 1800000
                 THEN ts_epoch_ms END) OVER (
            PARTITION BY canonical_id ORDER BY ts_epoch_ms
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_start_ms
    FROM sequenced
)
SELECT
    canonical_id,
    session_start_ms,
    CAST(epoch_ms(session_start_ms) AS DATE) AS day,
    event_type,
    product_id,
    price_cents
FROM assigned;

CREATE TEMP TABLE session_starts AS
SELECT DISTINCT canonical_id, session_start_ms, day FROM session_events;

CREATE TEMP TABLE session_credit AS
WITH click_winners AS (
    SELECT
        canonical_id,
        ts_epoch_ms,
        -- Compare click IDs as UTF-8 bytes for same-timestamp ties.
        arg_min(campaign_id, encode(click_id)) AS campaign_id
    FROM clicks
    GROUP BY canonical_id, ts_epoch_ms
)
SELECT
    s.canonical_id,
    s.session_start_ms,
    s.day,
    CASE WHEN s.session_start_ms - c.ts_epoch_ms <= 604800000
         THEN c.campaign_id ELSE 'organic' END AS campaign_id
FROM session_starts AS s
ASOF LEFT JOIN click_winners AS c
    ON s.canonical_id = c.canonical_id AND s.session_start_ms >= c.ts_epoch_ms;

CREATE TEMP TABLE session_assist AS
WITH sequenced AS (
    SELECT
        canonical_id,
        campaign_id,
        ts_epoch_ms,
        lag(ts_epoch_ms) OVER (
            PARTITION BY canonical_id, campaign_id ORDER BY ts_epoch_ms
        ) AS previous_ts
    FROM clicks
), assigned AS (
    SELECT
        canonical_id,
        campaign_id,
        ts_epoch_ms,
        max(CASE WHEN previous_ts IS NULL OR ts_epoch_ms - previous_ts > 604800000
                 THEN ts_epoch_ms END) OVER (
            PARTITION BY canonical_id, campaign_id ORDER BY ts_epoch_ms
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS first_click_ms
    FROM sequenced
), coverage AS (
    -- Merge overlapping inclusive intervals for a person and campaign before
    -- joining sessions, so repeated clicks yield one assist per campaign.
    SELECT
        canonical_id,
        campaign_id,
        first_click_ms,
        max(ts_epoch_ms) + 604800000 AS expires_ms
    FROM assigned
    GROUP BY canonical_id, campaign_id, first_click_ms
)
SELECT s.canonical_id, s.session_start_ms, s.day, c.campaign_id
FROM session_starts AS s
JOIN coverage AS c
    ON s.canonical_id = c.canonical_id
    AND s.session_start_ms BETWEEN c.first_click_ms AND c.expires_ms;
