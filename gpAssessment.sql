CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    DISTRIBUTE BY (country) -- Optimal country distribution
);
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0), -- Checking for a positive price
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    DISTRIBUTE BY (category) -- Optimal categorisation
);
CREATE TABLE sales_transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
    purchase_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    quantity INTEGER NOT NULL CHECK (quantity > 0), -- Checking for positive quantity
    total_price DECIMAL(10, 2) GENERATED ALWAYS AS (price * quantity) STORED, -- Calculation of total costs
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    DISTRIBUTE BY (customer_id) -- Оптимальное распределение по идентификатору клиента
);
CREATE TABLE shipping_details (
    transaction_id INTEGER PRIMARY KEY REFERENCES sales_transactions(transaction_id) ON DELETE CASCADE,
    shipping_date TIMESTAMP,
    shipping_address VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    tracking_number VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    DISTRIBUTE BY (city) -- Optimal city distribution
);


CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_sales_transactions_purchase_date ON sales_transactions(purchase_date);
CREATE INDEX idx_sales_transactions_customer_id ON sales_transactions(customer_id);
CREATE INDEX idx_sales_transactions_product_id ON sales_transactions(product_id);
CREATE INDEX idx_shipping_details_transaction_id ON shipping_details(transaction_id);


-- Calculate the total sales amount and the total number of transactions for each month.
SELECT 
    DATE_TRUNC('month', purchase_date) AS month,
    SUM(total_price) AS total_sales_amount,
    COUNT(transaction_id) AS total_transactions
FROM 
    sales_transactions
GROUP BY 
    month
ORDER BY 
    month;

-- Calculate the 3-month moving average of sales amount for each month. 
-- The moving average should be calculated based on the sales data 
-- from the previous 3 months (including the current month).
SELECT 
    DATE_TRUNC('month', purchase_date) AS month,
    SUM(total_price) AS total_sales_amount,
    AVG(SUM(total_price)) OVER (
        ORDER BY DATE_TRUNC('month', purchase_date)
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_average
FROM 
    sales_transactions
GROUP BY 
    month
ORDER BY 
    month;
