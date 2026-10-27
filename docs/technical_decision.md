# Technical Decisions Document

## 1. Analytical Repository Model Design
A **Star Schema** model was implemented in PostgreSQL to optimize the analytical queries required by the business.
*   **Facts:** Metrics were explicitly separated into `fact_sales` (transactions with `Quantity > 0`) and `fact_returns` (transactions with `Quantity <= 0`), isolating gross revenue behavior from return adjustments.
*   **Dimensions:** `dim_products` and `dim_customers` were created to store unique descriptive attributes, avoiding redundancy in the fact tables.
*   **Logs:** A `log_rejections` table was implemented to audit discarded records during the cleaning process without contaminating the analytical model.

## 2. Ambiguity Resolution & Business Rules
*   **Null Values in CustomerID:** These were not deleted. The value `'UNKNOWN'` was imputed (along with `'Unknown'` for the country) to avoid losing the revenue associated with guest checkout purchases.
*   **Invalid Prices (`UnitPrice <= 0`):** Rejected from fact tables as they are considered system errors or free samples with no financial impact. They were stored in `log_rejections` with an explicit reason.
*   **Inconsistent Descriptions:** Resolved by calculating the statistical mode (most frequent value) of the description for each `StockCode`. All descriptions were replaced by their corresponding mode, ensuring a single code has a single unique name.
*   **Categorization:** A keyword extraction function (SHIRT, BAG, PEN, USB, etc.) was applied directly over the standardized description to classify products into Clothing, Home, Stationery, Electronics, and Others.

## 3. DAG Idempotency Guarantee
Idempotency is guaranteed at the database load level. Before inserting the processed DataFrames using `.to_sql(if_exists='append')`, the pipeline executes a `TRUNCATE TABLE [table] CASCADE;` command within a transaction block. This ensures that executing the DAG multiple times with the same source file will not duplicate records, always maintaining an accurate snapshot of the data processed in the latest run.