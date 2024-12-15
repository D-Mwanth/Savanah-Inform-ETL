DROP TABLE IF EXISTS category_sales_summary;

CREATE TABLE category_sales_summary (
    category VARCHAR(255),
    total_sales DECIMAL(10, 2),
    total_items_sold INT,
    PRIMARY KEY (category)
);

INSERT INTO category_sales_summary
    WITH combine_trans_prod AS (
        SELECT
            c.cart_id,
            c.user_id,
            c.product_id,
            c.quantity,
            COALESCE(c.price, p.price) price,
            c.total_cart_value,
            p.name product_name,
            p.category,
            p.brand
        FROM carts_table c
        LEFT JOIN products_table p
            ON c.product_id = p.product_id
    )
    SELECT
        category,
        AVG(total_cart_value) AS total_sales,
        SUM(quantity)::INT as total_items_sold
    FROM combine_trans_prod
    WHERE category IS NOT NULL
    GROUP BY category;