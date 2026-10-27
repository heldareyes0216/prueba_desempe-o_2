-- 1. Monthly evolution of net sales (discounting returns)
WITH monthly_sales AS (
    SELECT DATE_TRUNC('month', transaction_date) AS month, SUM(gross_revenue) AS gross_sales
    FROM fact_sales GROUP BY 1
),
monthly_returns AS (
    SELECT DATE_TRUNC('month', return_date) AS month, SUM(ABS(net_revenue_adjustment)) AS returns_value
    FROM fact_returns GROUP BY 1
)
SELECT 
    s.month, 
    s.gross_sales, 
    COALESCE(r.returns_value, 0) AS total_returns,
    (s.gross_sales - COALESCE(r.returns_value, 0)) AS net_sales
FROM monthly_sales s
LEFT JOIN monthly_returns r ON s.month = r.month
ORDER BY s.month;

-- 2. Categories with the highest gross revenue and highest return rate
SELECT 
    p.category,
    SUM(s.gross_revenue) AS total_gross_revenue,
    COALESCE(SUM(ABS(r.quantity)), 0) * 100.0 / NULLIF(SUM(s.quantity), 0) AS return_rate_percentage
FROM dim_products p
LEFT JOIN fact_sales s ON p.product_code = s.product_code
LEFT JOIN fact_returns r ON p.product_code = r.product_code
GROUP BY p.category
ORDER BY total_gross_revenue DESC;

-- 3. Top 10 products with highest net revenue and Top 10 with highest return rate
-- (A) Top 10 Net Revenue
SELECT 
    p.product_name,
    SUM(s.gross_revenue) - COALESCE(SUM(ABS(r.net_revenue_adjustment)), 0) AS net_revenue
FROM dim_products p
JOIN fact_sales s ON p.product_code = s.product_code
LEFT JOIN fact_returns r ON p.product_code = r.product_code
GROUP BY p.product_name
ORDER BY net_revenue DESC
LIMIT 10;

-- (B) Top 10 Return Rate (minimum 10 units sold for statistical significance)
SELECT 
    p.product_name,
    COALESCE(SUM(ABS(r.quantity)), 0) * 100.0 / NULLIF(SUM(s.quantity), 0) AS return_rate
FROM dim_products p
JOIN fact_sales s ON p.product_code = s.product_code
LEFT JOIN fact_returns r ON p.product_code = r.product_code
GROUP BY p.product_name
HAVING SUM(s.quantity) > 10
ORDER BY return_rate DESC
LIMIT 10;

-- 4. Countries with the most transactions and average ticket variation
SELECT 
    c.country,
    COUNT(DISTINCT s.invoice_number) AS total_transactions,
    SUM(s.gross_revenue) / COUNT(DISTINCT s.invoice_number) AS average_ticket
FROM dim_customers c
JOIN fact_sales s ON c.customer_id = s.customer_id
GROUP BY c.country
ORDER BY total_transactions DESC;

-- 5. Purchasing behavior: Identified vs. Unidentified customers
SELECT 
    CASE WHEN customer_id = 'UNKNOWN' THEN 'Unidentified' ELSE 'Identified' END AS customer_type,
    COUNT(DISTINCT invoice_number) AS total_invoices,
    SUM(quantity) AS total_items_bought,
    SUM(gross_revenue) AS total_revenue,
    SUM(gross_revenue) / COUNT(DISTINCT invoice_number) AS avg_ticket_per_invoice
FROM fact_sales
GROUP BY CASE WHEN customer_id = 'UNKNOWN' THEN 'Unidentified' ELSE 'Identified' END;

-- 6. Products with inconsistent descriptions and total unique product codes
-- (A) Products without description (using our replacement rule)
SELECT product_code, product_name 
FROM dim_products 
WHERE product_name = 'NO DESCRIPTION';

-- (B) Total unique product codes
SELECT COUNT(DISTINCT product_code) AS total_unique_products 
FROM dim_products;

-- 7. Data-Driven Business Recommendation
-- (Query to analyze the "UNKNOWN" segment generating untrackable sales)
SELECT 
    c.country,
    SUM(s.gross_revenue) AS revenue,
    COUNT(DISTINCT s.customer_id) AS distinct_customers
FROM dim_customers c
JOIN fact_sales s ON c.customer_id = s.customer_id
WHERE c.country = 'Unknown' OR c.customer_id = 'UNKNOWN'
GROUP BY c.country;
