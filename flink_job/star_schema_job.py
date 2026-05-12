import os

from pyflink.table import EnvironmentSettings, TableEnvironment


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mock_data")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/bigdata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "flink")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "flink")


def jdbc_options(table_name: str) -> str:
    return f"""
        'connector' = 'jdbc',
        'url' = '{POSTGRES_URL}',
        'table-name' = '{table_name}',
        'username' = '{POSTGRES_USER}',
        'password' = '{POSTGRES_PASSWORD}',
        'driver' = 'org.postgresql.Driver',
        'sink.buffer-flush.max-rows' = '200',
        'sink.buffer-flush.interval' = '1s'
    """


def main():
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(settings)
    table_env.get_config().set("parallelism.default", "4")

    table_env.execute_sql(
        f"""
        CREATE TABLE kafka_mock_data (
            id STRING,
            customer_first_name STRING,
            customer_last_name STRING,
            customer_age STRING,
            customer_email STRING,
            customer_country STRING,
            customer_postal_code STRING,
            customer_pet_type STRING,
            customer_pet_name STRING,
            customer_pet_breed STRING,
            seller_first_name STRING,
            seller_last_name STRING,
            seller_email STRING,
            seller_country STRING,
            seller_postal_code STRING,
            product_name STRING,
            product_category STRING,
            product_price STRING,
            product_quantity STRING,
            sale_date STRING,
            sale_customer_id STRING,
            sale_seller_id STRING,
            sale_product_id STRING,
            sale_quantity STRING,
            sale_total_price STRING,
            store_name STRING,
            store_location STRING,
            store_city STRING,
            store_state STRING,
            store_country STRING,
            store_phone STRING,
            store_email STRING,
            pet_category STRING,
            product_weight STRING,
            product_color STRING,
            product_size STRING,
            product_brand STRING,
            product_material STRING,
            product_description STRING,
            product_rating STRING,
            product_reviews STRING,
            product_release_date STRING,
            product_expiry_date STRING,
            supplier_name STRING,
            supplier_contact STRING,
            supplier_email STRING,
            supplier_phone STRING,
            supplier_address STRING,
            supplier_city STRING,
            supplier_country STRING,
            `_source_file` STRING,
            `_source_line` INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-star-schema',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE dim_customer (
            customer_id BIGINT,
            first_name STRING,
            last_name STRING,
            age INT,
            email STRING,
            country STRING,
            postal_code STRING,
            pet_type STRING,
            pet_name STRING,
            pet_breed STRING,
            PRIMARY KEY (customer_id) NOT ENFORCED
        ) WITH ({jdbc_options("dim_customer")})
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE dim_seller (
            seller_id BIGINT,
            first_name STRING,
            last_name STRING,
            email STRING,
            country STRING,
            postal_code STRING,
            PRIMARY KEY (seller_id) NOT ENFORCED
        ) WITH ({jdbc_options("dim_seller")})
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE dim_product (
            product_id BIGINT,
            product_name STRING,
            category STRING,
            price DECIMAL(12, 2),
            quantity INT,
            pet_category STRING,
            weight DECIMAL(12, 2),
            color STRING,
            size_name STRING,
            brand STRING,
            material STRING,
            description STRING,
            rating DECIMAL(3, 1),
            reviews INT,
            release_date STRING,
            expiry_date STRING,
            PRIMARY KEY (product_id) NOT ENFORCED
        ) WITH ({jdbc_options("dim_product")})
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE dim_store (
            store_id STRING,
            store_name STRING,
            store_location STRING,
            city STRING,
            state_name STRING,
            country STRING,
            phone STRING,
            email STRING,
            PRIMARY KEY (store_id) NOT ENFORCED
        ) WITH ({jdbc_options("dim_store")})
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE dim_supplier (
            supplier_id STRING,
            supplier_name STRING,
            contact_name STRING,
            email STRING,
            phone STRING,
            address STRING,
            city STRING,
            country STRING,
            PRIMARY KEY (supplier_id) NOT ENFORCED
        ) WITH ({jdbc_options("dim_supplier")})
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE fact_sales (
            sale_id STRING,
            customer_id BIGINT,
            seller_id BIGINT,
            product_id BIGINT,
            store_id STRING,
            supplier_id STRING,
            sale_date STRING,
            sale_quantity INT,
            sale_total_price DECIMAL(12, 2),
            source_file STRING,
            source_line INT,
            PRIMARY KEY (sale_id) NOT ENFORCED
        ) WITH ({jdbc_options("fact_sales")})
        """
    )

    statement_set = table_env.create_statement_set()

    statement_set.add_insert_sql(
        """
        INSERT INTO dim_customer
        SELECT DISTINCT
            CAST(sale_customer_id AS BIGINT),
            customer_first_name,
            customer_last_name,
            CAST(customer_age AS INT),
            customer_email,
            customer_country,
            customer_postal_code,
            customer_pet_type,
            customer_pet_name,
            customer_pet_breed
        FROM kafka_mock_data
        WHERE sale_customer_id IS NOT NULL
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO dim_seller
        SELECT DISTINCT
            CAST(sale_seller_id AS BIGINT),
            seller_first_name,
            seller_last_name,
            seller_email,
            seller_country,
            seller_postal_code
        FROM kafka_mock_data
        WHERE sale_seller_id IS NOT NULL
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO dim_product
        SELECT DISTINCT
            CAST(sale_product_id AS BIGINT),
            product_name,
            product_category,
            CAST(product_price AS DECIMAL(12, 2)),
            CAST(product_quantity AS INT),
            pet_category,
            CAST(product_weight AS DECIMAL(12, 2)),
            product_color,
            product_size,
            product_brand,
            product_material,
            product_description,
            CAST(product_rating AS DECIMAL(3, 1)),
            CAST(product_reviews AS INT),
            product_release_date,
            product_expiry_date
        FROM kafka_mock_data
        WHERE sale_product_id IS NOT NULL
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO dim_store
        SELECT DISTINCT
            COALESCE(store_name, 'unknown-store'),
            store_name,
            store_location,
            store_city,
            store_state,
            store_country,
            store_phone,
            store_email
        FROM kafka_mock_data
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO dim_supplier
        SELECT DISTINCT
            COALESCE(supplier_name, 'unknown-supplier'),
            supplier_name,
            supplier_contact,
            supplier_email,
            supplier_phone,
            supplier_address,
            supplier_city,
            supplier_country
        FROM kafka_mock_data
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO fact_sales
        SELECT
            CONCAT(COALESCE(`_source_file`, 'unknown'), ':', CAST(`_source_line` AS STRING)),
            CAST(sale_customer_id AS BIGINT),
            CAST(sale_seller_id AS BIGINT),
            CAST(sale_product_id AS BIGINT),
            COALESCE(store_name, 'unknown-store'),
            COALESCE(supplier_name, 'unknown-supplier'),
            sale_date,
            CAST(sale_quantity AS INT),
            CAST(sale_total_price AS DECIMAL(12, 2)),
            `_source_file`,
            `_source_line`
        FROM kafka_mock_data
        WHERE id IS NOT NULL
        """
    )

    statement_set.execute().wait()


if __name__ == "__main__":
    main()
