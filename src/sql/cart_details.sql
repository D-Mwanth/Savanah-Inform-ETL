DROP TABLE IF EXISTS cart_details;

CREATE TABLE IF NOT EXISTS cart_details (
    cart_id INT,
    user_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10, 2),
    total_cart_value DECIMAL(10, 2),
    PRIMARY KEY (cart_id, product_id)
);

INSERT INTO cart_details
SELECT 
    cart_id,
    user_id,
    product_id,
    quantity,
    price,
    total_cart_value
FROM user_transactions
ORDER BY cart_id;