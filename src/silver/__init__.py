"""Silver layer: cleans Bronze prices and weather into typed, deduplicated UTC tables (PySpark).

Modules are imported directly (e.g. `src.silver.prices`) so that importing this package never
starts or requires Spark. Design: docs/plans/silver_layer.md.
"""
