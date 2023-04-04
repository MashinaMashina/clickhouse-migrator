create table defaultdb.category_products on cluster '{cluster}' (
    wb_category_id UInt64,
    created_at_month TIMESTAMP,
    wb_product_id UInt64,
    days_of_month_mask UInt32,
    txt TEXT
) engine = ReplacingMergeTree PARTITION BY created_at_month
ORDER BY (wb_category_id, created_at_month, wb_product_id) SETTINGS index_granularity = 1024;
--
-- insert into defaultdb.category_products (wb_category_id) values(1); - commented
-- any query with semi-colon;
--
insert into defaultdb.category_products (txt) values("-- value;");
--
create table if not exists defaultdb.category_products_current on cluster '{cluster}' (
    wb_category_id UInt64,
    created_at_month TIMESTAMP,
    wb_product_id UInt64,
    days_of_month_mask UInt32
) engine = ReplacingMergeTree PARTITION BY created_at_month
ORDER BY (wb_category_id, created_at_month, wb_product_id) SETTINGS index_granularity = 1024;
--
create table defaultdb.category_products_dist on cluster '{cluster}' (
wb_category_id UInt64,
created_at_month TIMESTAMP,
wb_product_id UInt64,
days_of_month_mask UInt32
) engine = Distributed(
    '{cluster}',
    'defaultdb',
    'category_products',
    intHash32(wb_category_id)
);
--
create table defaultdb.category_products_current_dist on cluster '{cluster}' (
wb_category_id UInt64,
created_at_month TIMESTAMP,
wb_product_id UInt64,
days_of_month_mask UInt32
) engine = Distributed(
    '{cluster}',
    'defaultdb',
    'category_products_current',
    intHash32(wb_category_id)
) ;
--
CREATE MATERIALIZED VIEW IF NOT EXISTS defaultdb.category_products_view ON CLUSTER '{cluster}' TO defaultdb.category_products_dist AS
SELECT wb_category_id as wb_category_id,
    toStartOfMonth(created_at_day) as created_at_month,
    wb_id as wb_product_id,
    toUInt32(
        groupBitOr(toUInt32(pow(2, toDayOfMonth(created_at_day))))
    ) as days_of_month_mask
FROM defaultdb.products_v1
WHERE created_at_month >= toStartOfMonth(now() - interval 1 month)
GROUP BY created_at_month,
    wb_category_id,
    wb_id;
--
alter table defaultdb.category_products on cluster '{cluster}' add index idx_days_of_month_mask days_of_month_mask type minmax granularity 1;
alter table defaultdb.category_products on cluster '{cluster}' materialize index idx_days_of_month_mask;