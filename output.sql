CREATE TEMP TABLE session_totals AS
SELECT
    canonical_id,
    session_start_ms,
    day,
    count(*) AS events,
    count(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    coalesce(sum(price_cents) FILTER (WHERE event_type = 'purchase'), 0)::BIGINT AS revenue_cents
FROM session_events
GROUP BY canonical_id, session_start_ms, day;

CREATE TEMP TABLE user_daily AS
WITH purchase_categories AS (
    SELECT
        e.canonical_id,
        e.day,
        coalesce(p.category, 'unknown') AS category,
        sum(e.price_cents) AS category_revenue
    FROM session_events AS e
    LEFT JOIN raw_products AS p USING (product_id)
    WHERE e.event_type = 'purchase'
    GROUP BY e.canonical_id, e.day, coalesce(p.category, 'unknown')
), top_categories AS (
    SELECT canonical_id, day, category
    FROM purchase_categories
    QUALIFY row_number() OVER (
        PARTITION BY canonical_id, day
        ORDER BY category_revenue DESC, encode(category)
    ) = 1
), daily_totals AS (
    SELECT
        canonical_id,
        day,
        count(*) AS sessions,
        sum(events)::BIGINT AS events,
        sum(revenue_cents)::BIGINT AS revenue_cents
    FROM session_totals
    GROUP BY canonical_id, day
)
SELECT
    d.canonical_id AS user_id,
    d.day,
    d.sessions,
    d.events,
    d.revenue_cents,
    c.category AS top_category
FROM daily_totals AS d
LEFT JOIN top_categories AS c USING (canonical_id, day);

CREATE TEMP TABLE campaign_daily AS
WITH contributions AS (
    SELECT
        c.campaign_id,
        c.day,
        count(*) AS sessions,
        count(*) FILTER (WHERE s.purchases > 0) AS purchasing_sessions,
        sum(s.revenue_cents)::BIGINT AS revenue_cents,
        0::BIGINT AS assisted_sessions
    FROM session_credit AS c
    JOIN session_totals AS s USING (canonical_id, session_start_ms)
    GROUP BY c.campaign_id, c.day

    UNION ALL

    SELECT
        campaign_id,
        day,
        0::BIGINT AS sessions,
        0::BIGINT AS purchasing_sessions,
        0::BIGINT AS revenue_cents,
        count(*) AS assisted_sessions
    FROM session_assist
    GROUP BY campaign_id, day
), daily_totals AS (
    SELECT
        campaign_id,
        day,
        sum(sessions)::BIGINT AS sessions,
        sum(purchasing_sessions)::BIGINT AS purchasing_sessions,
        sum(revenue_cents)::BIGINT AS revenue_cents,
        sum(assisted_sessions)::BIGINT AS assisted_sessions
    FROM contributions
    GROUP BY campaign_id, day
)
SELECT
    d.campaign_id,
    CASE WHEN d.campaign_id = 'organic' THEN 'organic' ELSE c.channel END AS channel,
    d.day,
    d.sessions,
    d.purchasing_sessions,
    d.revenue_cents,
    d.assisted_sessions
FROM daily_totals AS d
LEFT JOIN raw_campaigns AS c USING (campaign_id);
