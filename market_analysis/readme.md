# Marketing-ETL

## Overview

Marketing-ETL is a robust Data Engineering project that leverages dbt (Data Build Tool) and Snowflake to transform raw marketing, customer, and sales data into actionable, business-ready models. The project implements a best-practice Medallion Architecture to ensure data quality, traceability, and performance.

## Architecture

The data pipeline is structured into three primary layers:

### 1. Bronze Layer (Raw Data)
The Bronze layer ingests raw data directly from source systems. This layer acts as the historical archive and the foundational layer for all downstream transformations.
- **Sources**: Customer Relationship Management (CRM) and Enterprise Resource Planning (ERP) systems.
- **Models**: `crm_cust_info_bronze`, `crm_prd_info_bronze`, `crm_sales_details_bronze`, `erp_cust_az12_bronze`, `erp_loc_a101_bronze`, `erp_px_cat_g1v2_bronze`.

### 2. Silver Layer (Cleansed and Conformed)
The Silver layer cleanses, normalizes, and conforms the raw Bronze data. It resolves variations in data types, handles null values, and enforces naming conventions across different source schemas.
- **Models**: `crm_cust_info_silver`, `crm_prd_info_silver`, `crm_sales_details_silver`, `erp_cust_az12_silver`, `erp_loc_a101_silver`, `erp_px_cat_g1v2_silver`.

### 3. Gold Layer (Business and Reporting)
The Gold layer is optimized for reporting and analytics. It contains dimensional models (star schema) and a denormalized One-Big-Table (OBT) for seamless integration with Business Intelligence tools.
- **Dimensions**: `dim_customer_gold`, `dim_product_gold`.
- **Facts**: `fact_sales_gold`.
- **Reporting**: `obt` (One-Big-Table combining facts and dimensions for streamlined querying).

## Tech Stack

- **Transformation**: dbt Core
- **Data Warehouse**: Snowflake
- **Language**: SQL, Python (Dependencies management via `uv` / `pyproject.toml`)

## Lineage Graph



[<img width="1817" height="792" alt="image" src="https://github.com/user-attachments/assets/33e07ad0-999f-4b27-863a-21e0891bddda" />
]

## Getting Started

### Prerequisites

- Python 3.10 or higher
- dbt-core (>= 1.11.7)
- dbt-snowflake (>= 1.11.3)
- Snowflake account and valid credentials

### Installation and Setup

1. **Clone the repository** and navigate to the project directory.

2. **Install dependencies**:
   This project uses a `pyproject.toml` file. You can install the required packages using pip or uv:
   ```bash
   pip install .
   ```

3. **Configure your dbt profile**:
   Ensure your `~/.dbt/profiles.yml` contains the `dbt_marketing` profile with your Snowflake connection details.

   Example:
   ```yaml
   dbt_marketing:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: <your_snowflake_account>
         user: <your_username>
         password: <your_password>
         role: <your_role>
         database: <your_database>
         warehouse: <your_warehouse>
         schema: <your_schema>
         threads: 4
   ```

4. **Verify the connection**:
   Navigate to the `dbt_marketing` directory and test the connection:
   ```bash
   cd dbt_marketing
   dbt debug
   ```

## Execution

To build the entire project (run models, test, and seed data):
```bash
dbt build
```

To run a specific layer (e.g., the Gold layer):
```bash
dbt run --select gold
```

To generate and view the project documentation:
```bash
dbt docs generate
dbt docs serve
```

## Testing

The project incorporates dbt tests defined in `schema.yml` files (for example, within the Silver and Gold directories) to ensure data integrity, uniqueness, and non-null constraints on critical columns.

