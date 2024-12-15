DROP TABLE IF EXISTS user_transactions;

CREATE TABLE IF NOT EXISTS user_transactions (
user_id INT,
cart_id INT,
first_name VARCHAR(255),
last_name VARCHAR(255),
gender VARCHAR(50),
age INT,
street VARCHAR(255),
city VARCHAR(255),
postal_code VARCHAR(20),
product_id INT,
quantity INT,
price DECIMAL(10, 2),
total_cart_value DECIMAL(10, 2),
PRIMARY KEY (user_id, cart_id, product_id)
);

INSERT INTO user_transactions
SELECT
    u.user_id,
    c.cart_id,
    u.first_name,
    u.last_name,
    u.gender,
    u.age,
    u.street,
    u.city,
    u.postal_code,
    c.product_id,
    c.quantity,
    c.price,
    c.total_cart_value
FROM users_table u
FULL JOIN carts_table c
    ON u.user_id = c.user_id
WHERE c.cart_id IS NOT NULL          
ORDER BY u.user_id;