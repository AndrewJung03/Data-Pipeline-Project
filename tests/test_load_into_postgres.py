import etl.load_into_postgres as loader

def test_functions_test():
    assert hasattr(loader, "get_connection")
    assert hasattr(loader, "drop_tables")
    assert hasattr(loader, "create_tables")

def test_create_tables_sql():
    with open("etl/load_into_postgres.py", "r") as f:
        content = f.read()
    assert "CREATE TABLE IF NOT EXISTS hosts" in content
    assert "CREATE TABLE IF NOT EXISTS locations" in content
    assert "CREATE TABLE IF NOT EXISTS room_types" in content
    assert "CREATE TABLE IF NOT EXISTS listings" in content 

def test_drop_tables_sql_present():
    with open("etl/load_into_postgres.py", "r") as f:
        content = f.read()

    assert "DROP TABLE IF EXISTS listings" in content
    assert "DROP TABLE IF EXISTS hosts" in content
    assert "DROP TABLE IF EXISTS locations" in content
    assert "DROP TABLE IF EXISTS room_types" in content


