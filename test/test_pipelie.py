import duckdb

def test_metrics_not_empty():
    conn = duckdb.connect("/app/scripts/mydatabase.db")
    result = conn.execute("SELECT COUNT(*) FROM report_layer.metrics_table_name").fetchone()[0]
    assert result > 0, "La table des métriques est vide"