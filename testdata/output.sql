create table category_products  (
    wb_category_id UInt64,
    created_at_month TIMESTAMP,
    wb_product_id UInt64,
    days_of_month_mask UInt32
) engine = ReplacingMergeTree PARTITION BY created_at_month
ORDER BY (wb_category_id, created_at_month, wb_product_id) SETTINGS index_granularity = 1024;
--
create table if not exists category_products_current  (
    wb_category_id UInt64,
    created_at_month TIMESTAMP,
    wb_product_id UInt64,
    days_of_month_mask UInt32
) engine = ReplacingMergeTree PARTITION BY created_at_month
ORDER BY (wb_category_id, created_at_month, wb_product_id) SETTINGS index_granularity = 1024;
--
create table category_products_dist  (
wb_category_id UInt64,
created_at_month TIMESTAMP,
wb_product_id UInt64,
days_of_month_mask UInt32
) engine = MergeTree order by (wb_category_id, created_at_month, wb_product_id);
--
create table category_products_current_dist  (
wb_category_id UInt64,
created_at_month TIMESTAMP,
wb_product_id UInt64,
days_of_month_mask UInt32
) engine = MergeTree order by (wb_category_id, created_at_month, wb_product_id);
--
CREATE MATERIALIZED VIEW IF NOT EXISTS category_products_view  TO category_products_dist AS
SELECT wb_category_id as wb_category_id,
    toStartOfMonth(created_at_day) as created_at_month,
    wb_id as wb_product_id,
    toUInt32(
        groupBitOr(toUInt32(pow(2, toDayOfMonth(created_at_day))))
    ) as days_of_month_mask
FROM products_v1
WHERE created_at_month >= toStartOfMonth(now() - interval 1 month)
GROUP BY created_at_month,
    wb_category_id,
    wb_id;
--
alter table category_products  add index idx_days_of_month_mask days_of_month_mask type minmax granularity 1;
alter table category_products  materialize index idx_days_of_month_mask;