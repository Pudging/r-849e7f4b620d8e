-- Decode timestamps only after deduplication. The zone is applied explicitly
-- to a naive date/time so the connection's time zone never affects UTC output.
CREATE OR REPLACE TEMP MACRO unsigned_ascii(value) AS (
    CASE WHEN regexp_full_match(value, '[0-9]+') THEN
        try_cast(coalesce(nullif(ltrim(value, '0'), ''), '0') AS BIGINT)
    END
);

CREATE OR REPLACE TEMP MACRO timestamp_ms(value) AS (
    CASE
        WHEN regexp_full_match(value, '[0-9]+') THEN unsigned_ascii(value)
        WHEN regexp_full_match(value,
            '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])')
        THEN epoch_ms(try_strptime(left(value, 19), '%Y-%m-%dT%H:%M:%S'))
            + CASE substr(value, 20, 1)
                WHEN 'Z' THEN 0
                ELSE (CASE substr(value, 20, 1) WHEN '-' THEN 1 ELSE -1 END)
                    * (try_cast(substr(value, 21, 2) AS BIGINT) * 3600000
                        + try_cast(substr(value, 24, 2) AS BIGINT) * 60000)
              END
    END
);

CREATE TEMP TABLE events AS
WITH deduplicated AS (
    SELECT * FROM raw_events
    WHERE event_id <> ''
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts) = 1
), parsed AS (
    SELECT * EXCLUDE (price_cents),
        CASE WHEN event_type = 'purchase' THEN unsigned_ascii(price_cents) END AS price_cents,
        timestamp_ms(ts) AS ts_epoch_ms,
        epoch_ms(try_strptime(left(ingest_ts, 19), '%Y-%m-%dT%H:%M:%S')) AS ingest_ms
    FROM deduplicated
    WHERE user_id <> '' AND product_id <> ''
        AND event_type IN ('view', 'cart', 'purchase')
)
SELECT coalesce(identity_map.canonical_id, parsed.user_id) AS canonical_id,
    ts_epoch_ms, event_type, product_id, price_cents
FROM parsed
CROSS JOIN input_window
LEFT JOIN identity_map ON identity_map.user_id = parsed.user_id
WHERE ts_epoch_ms >= window_start_ms AND ts_epoch_ms < window_end_ms
    AND ts_epoch_ms >= ingest_ms - 86400000
    AND (event_type <> 'purchase' OR price_cents BETWEEN 1 AND 10000000);

CREATE TEMP TABLE clicks AS
WITH deduplicated AS (
    -- Official inputs guarantee that all copies of a click have identical values.
    SELECT DISTINCT click_id, user_id, campaign_id, ts FROM raw_clicks
    WHERE click_id <> ''
), parsed AS (
    SELECT deduplicated.*, timestamp_ms(ts) AS ts_epoch_ms
    FROM deduplicated
    SEMI JOIN raw_campaigns USING (campaign_id)
    WHERE user_id <> ''
)
SELECT coalesce(identity_map.canonical_id, parsed.user_id) AS canonical_id,
    ts_epoch_ms, click_id, campaign_id
FROM parsed
CROSS JOIN input_window
LEFT JOIN identity_map ON identity_map.user_id = parsed.user_id
WHERE ts_epoch_ms >= window_start_ms - 604800000 AND ts_epoch_ms < window_end_ms;
