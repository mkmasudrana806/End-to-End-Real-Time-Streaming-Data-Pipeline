create table if not exists product_view_window (
    window_start timestamp,
    window_end timestamp,
    product_id text,
    view_count bigint
);

create table if not exists category_activity_window (
    window_start timestamp,
    window_end timestamp,
    category text,
    event_count bigint
);

create table if not exists category_revenue_window (
    window_start timestamp,
    window_end timestamp,
    category text,
    total_revenue double precision
);
