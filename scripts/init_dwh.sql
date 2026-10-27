
CREATE TABLE IF NOT EXISTS dim_products (
    product_code VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    transaction_id SERIAL PRIMARY KEY,
    invoice_number VARCHAR(50) NOT NULL,
    product_code VARCHAR(50) NOT NULL
        REFERENCES dim_products(product_code),
    customer_id VARCHAR(50) NOT NULL
        REFERENCES dim_customers(customer_id),
    transaction_date TIMESTAMP NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    gross_revenue DECIMAL(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_returns (
    return_id SERIAL PRIMARY KEY,
    invoice_number VARCHAR(50) NOT NULL,
    product_code VARCHAR(50) NOT NULL
        REFERENCES dim_products(product_code),
    customer_id VARCHAR(50) NOT NULL
        REFERENCES dim_customers(customer_id),
    return_date TIMESTAMP,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    net_revenue_adjustment DECIMAL(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS log_rejections (
    log_id SERIAL PRIMARY KEY,
    source_file VARCHAR(255) NOT NULL,
    raw_record TEXT NOT NULL,
    rejection_reason TEXT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
