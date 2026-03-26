"""Benchmark profile metadata and DuckDB SQL generators for all six profiles."""

from __future__ import annotations

# ── Profile registry ──────────────────────────────────────────────────────
# Each entry maps a profile name to the SCD metadata needed by processors.

_PROFILE_META: dict[str, dict] = {
    "products": {
        "source_class": "sqldim.application.datasets.domains.ecommerce.ProductsSource",
        "natural_key": "product_id",
        "track_columns": ["name", "category", "price"],
    },
    "employees": {
        "source_class": "sqldim.application.datasets.domains.enterprise.EmployeesSource",
        "natural_key": "employee_id",
        "track_columns": ["title", "department"],
    },
    "customers": {
        "source_class": "sqldim.application.datasets.domains.ecommerce.CustomersSource",
        "natural_key": "customer_id",
        "track_columns": ["full_name", "email"],
    },
    "saas_users": {
        "source_class": "sqldim.application.datasets.domains.saas_growth.SaaSUsersSource",
        "natural_key": "user_id",
        "track_columns": ["plan_tier"],
    },
    "player_seasons": {
        "source_class": "sqldim.application.datasets.domains.nba_analytics.PlayerSeasonsSource",
        "natural_key": "player_id",
        "track_columns": ["scoring_class"],
    },
    "cnpj_empresa": {
        "source_class": None,
        "natural_key": "cnpj_basico",
        "track_columns": [
            "razao_social",
            "natureza_juridica",
            "porte_empresa",
            "capital_social",
        ],
        "metadata_columns": [
            "razao_social",
            "natureza_juridica",
            "porte_empresa",
            "capital_social",
        ],
        "ddl": """
            CREATE TABLE IF NOT EXISTS {table} (
                cnpj_basico         VARCHAR,
                valid_from          TIMESTAMPTZ,
                valid_to            TIMESTAMPTZ,
                is_current          BOOLEAN,
                metadata            JSON,
                metadata_diff       JSON,
                row_hash            VARCHAR
            )
        """,
    },
}


# ── DuckDB SQL generators ─────────────────────────────────────────────────
# Fast in-database generation via generate_series — no Python row loops.

_SQL_GENERATORS: dict[str, dict] = {
    "products": {
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                i                                                  AS product_id,
                concat('Product_', i)                              AS name,
                CASE (i % 5)
                    WHEN 0 THEN 'widgets'
                    WHEN 1 THEN 'gadgets'
                    WHEN 2 THEN 'tools'
                    WHEN 3 THEN 'accessories'
                    ELSE        'consumables'
                END                                                AS category,
                round(4.99 + (i % 1000) * 0.195, 2)               AS price
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                i                                                  AS product_id,
                concat('Product_', i, '_v2')                       AS name,
                CASE (i % 5)
                    WHEN 0 THEN 'widgets'
                    WHEN 1 THEN 'gadgets'
                    WHEN 2 THEN 'tools'
                    WHEN 3 THEN 'accessories'
                    ELSE        'consumables'
                END                                                AS category,
                round(4.99 + (i % 1000) * 0.195 * 1.15, 2)        AS price
            FROM generate_series(1, {changed}) t(i)
        """,
    },
    "employees": {
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                100 + i                                            AS employee_id,
                concat('Employee_', i)                             AS full_name,
                CASE (i % 5)
                    WHEN 0 THEN 'Engineer'
                    WHEN 1 THEN 'Analyst'
                    WHEN 2 THEN 'Manager'
                    WHEN 3 THEN 'Developer'
                    ELSE        'Associate'
                END                                                AS title,
                CASE (i % 7)
                    WHEN 0 THEN 'Engineering'
                    WHEN 1 THEN 'Finance'
                    WHEN 2 THEN 'Marketing'
                    WHEN 3 THEN 'Product'
                    WHEN 4 THEN 'HR'
                    WHEN 5 THEN 'Legal'
                    ELSE        'Sales'
                END                                                AS department
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                100 + i                                            AS employee_id,
                concat('Employee_', i)                             AS full_name,
                CASE (i % 5)
                    WHEN 0 THEN 'Senior Engineer'
                    WHEN 1 THEN 'Senior Analyst'
                    WHEN 2 THEN 'Senior Manager'
                    WHEN 3 THEN 'Lead Developer'
                    ELSE        'Senior Associate'
                END                                                AS title,
                CASE ((i + 1) % 7)
                    WHEN 0 THEN 'Engineering'
                    WHEN 1 THEN 'Finance'
                    WHEN 2 THEN 'Marketing'
                    WHEN 3 THEN 'Product'
                    WHEN 4 THEN 'HR'
                    WHEN 5 THEN 'Legal'
                    ELSE        'Sales'
                END                                                AS department
            FROM generate_series(1, {changed}) t(i)
        """,
    },
    "customers": {
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                i                                                  AS customer_id,
                concat('Customer_', i)                             AS full_name,
                concat('cust', i, '@bench.io')                     AS email,
                concat(i % 50, ' Main St')                         AS addr
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                i                                                  AS customer_id,
                concat('Customer_', i, '_upd')                     AS full_name,
                concat('cust', i, '_new@bench.io')                 AS email,
                concat(i % 50, ' New Ave')                         AS addr
            FROM generate_series(1, {changed}) t(i)
        """,
    },
    "saas_users": {
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                i                                                  AS user_id,
                concat('user', i, '@bench.io')                     AS email,
                CASE (i % 10)
                    WHEN 0 THEN 'pro'
                    WHEN 1 THEN 'enterprise'
                    ELSE        'free'
                END                                                AS plan_tier,
                CASE (i % 5)
                    WHEN 0 THEN 'organic'
                    WHEN 1 THEN 'ads'
                    WHEN 2 THEN 'referral'
                    WHEN 3 THEN 'content'
                    ELSE        'social'
                END                                                AS acq_source,
                CASE (i % 3)
                    WHEN 0 THEN 'desktop'
                    WHEN 1 THEN 'mobile'
                    ELSE        'tablet'
                END                                                AS device,
                (DATE '2020-01-01' + (i % 1825)::INTEGER)::VARCHAR         AS signup_date
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                i                                                  AS user_id,
                concat('user', i, '@bench.io')                     AS email,
                CASE (i % 10)
                    WHEN 0 THEN 'enterprise'
                    WHEN 1 THEN 'enterprise'
                    ELSE        'pro'
                END                                                AS plan_tier,
                CASE (i % 5)
                    WHEN 0 THEN 'organic'
                    WHEN 1 THEN 'ads'
                    WHEN 2 THEN 'referral'
                    WHEN 3 THEN 'content'
                    ELSE        'social'
                END                                                AS acq_source,
                CASE (i % 3)
                    WHEN 0 THEN 'desktop'
                    WHEN 1 THEN 'mobile'
                    ELSE        'tablet'
                END                                                AS device,
                (DATE '2020-01-01' + (i % 1825)::INTEGER)::VARCHAR         AS signup_date
            FROM generate_series(1, {changed}) t(i)
        """,
    },
    "player_seasons": {
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                i                                                  AS player_id,
                concat('Player_', i)                               AS player_name,
                2000 + (i % 25)                                    AS season,
                CASE (i % 4)
                    WHEN 0 THEN 'Elite'
                    WHEN 1 THEN 'Star'
                    WHEN 2 THEN 'Starter'
                    ELSE        'Rotation'
                END                                                AS scoring_class,
                round(5.0 + (i % 30), 1)                           AS pts_per_game,
                round(2.0 + (i % 10), 1)                           AS reb_per_game,
                round(1.0 + (i % 8), 1)                            AS ast_per_game
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                i                                                  AS player_id,
                concat('Player_', i)                               AS player_name,
                2000 + (i % 25)                                    AS season,
                CASE ((i + 1) % 4)
                    WHEN 0 THEN 'Elite'
                    WHEN 1 THEN 'Star'
                    WHEN 2 THEN 'Starter'
                    ELSE        'Rotation'
                END                                                AS scoring_class,
                round(5.0 + (i % 30) * 1.1, 1)                    AS pts_per_game,
                round(2.0 + (i % 10) * 1.05, 1)                   AS reb_per_game,
                round(1.0 + (i % 8) * 1.1, 1)                     AS ast_per_game
            FROM generate_series(1, {changed}) t(i)
        """,
    },
    "cnpj_empresa": {
        "snapshot_view": """
            CREATE OR REPLACE VIEW _bench_snapshot AS
            SELECT
                lpad(cast(i AS VARCHAR), 8, '0')           AS cnpj_basico,
                concat('EMPRESA ', i)                      AS razao_social,
                CASE (i % 10)
                    WHEN 0 THEN '2062'
                    WHEN 1 THEN '2240'
                    WHEN 2 THEN '3034'
                    ELSE         '2011'
                END                                        AS natureza_juridica,
                CASE (i % 5)
                    WHEN 0 THEN '49'
                    WHEN 1 THEN '05'
                    ELSE         '01'
                END                                        AS porte_empresa,
                round(1000.0 + (i % 100000) * 0.1, 2)     AS capital_social
            FROM generate_series(1, {n}) t(i)
        """,
        "event_view": """
            CREATE OR REPLACE VIEW _bench_events AS
            SELECT
                lpad(cast(i AS VARCHAR), 8, '0')           AS cnpj_basico,
                concat('EMPRESA ', i, ' ATUALIZADA')       AS razao_social,
                CASE (i % 10)
                    WHEN 0 THEN '2062'
                    WHEN 1 THEN '2240'
                    WHEN 2 THEN '3034'
                    ELSE         '2011'
                END                                        AS natureza_juridica,
                CASE (i % 5)
                    WHEN 0 THEN '03'
                    WHEN 1 THEN '05'
                    ELSE         '01'
                END                                        AS porte_empresa,
                round(1000.0 + (i % 100000) * 0.1 * 1.1, 2) AS capital_social
            FROM generate_series(1, {changed}) t(i)
        """,
    },
}
