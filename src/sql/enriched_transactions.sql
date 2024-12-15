DROP TABLE IF EXISTS enriched_transactions;

CREATE TABLE enriched_transactions (
    cart_id INT,
    user_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10, 2),
    total_cart_value DECIMAL(10, 2),
    product_name VARCHAR(255),
    category VARCHAR(255),
    brand VARCHAR(255),
    PRIMARY KEY (cart_id, product_id)
);

INSERT INTO enriched_transactions
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
        ON c.product_id = p.product_id;