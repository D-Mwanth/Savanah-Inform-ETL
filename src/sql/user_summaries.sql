DROP TABLE IF EXISTS user_summaries;

CREATE TABLE user_summaries (
    user_id INT,
    first_name VARCHAR(255),
    total_spend DECIMAL(10, 2),
    total_items INT,
    age INT,
    city VARCHAR(255),
    PRIMARY KEY (user_id)
);

INSERT INTO user_summaries
    SELECT
        user_id,
        first_name,
        ROUND(AVG(total_cart_value), 3) total_spend, -- total cart value
        SUM(quantity) total_items,
        age,
        city
    FROM user_transactions
    GROUP BY user_id, first_name, age, city
    ORDER BY user_id;