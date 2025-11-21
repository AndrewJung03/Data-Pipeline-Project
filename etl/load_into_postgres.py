import psycopg

def get_connection():
    """
    Create a psycopg3 connection to the database.
    Update credentials as necessary.
    """
    return psycopg.connect(
        host="localhost",
        dbname="airbnb",
        user="postgres",
        password="9660",
        port=5432
    )


#Drop tables

def drop_tables():
    conn = get_connection()
    cur = conn.cursor()  # type: ignore[attr-defined]

    print("Dropping existing tables (if they exist)...")

    # Drop in the correct dependency order:
    cur.execute("DROP TABLE IF EXISTS listings CASCADE;")
    cur.execute("DROP TABLE IF EXISTS hosts CASCADE;")
    cur.execute("DROP TABLE IF EXISTS locations CASCADE;")
    cur.execute("DROP TABLE IF EXISTS room_types CASCADE;")

    conn.commit()
    cur.close()
    conn.close()

    print("✔ Tables dropped successfully.\n")

#normalized

def create_tables():
    conn = get_connection()
    cur = conn.cursor()  # type: ignore[attr-defined]

    print("Creating normalized tables...")

    # HOSTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hosts (
            host_id BIGINT PRIMARY KEY,
            host_name TEXT
        );
    """)

    # LOCATIONS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            location_id SERIAL PRIMARY KEY,
            neighbourhood_group TEXT,
            neighbourhood TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        );
    """)

    # ROOM_TYPES
    cur.execute("""
        CREATE TABLE IF NOT EXISTS room_types (
            room_type_id SERIAL PRIMARY KEY,
            room_type TEXT UNIQUE
        );
    """)

    # LISTINGS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            listing_id BIGINT PRIMARY KEY,
            name TEXT,

            host_id BIGINT REFERENCES hosts(host_id),
            location_id INT REFERENCES locations(location_id),
            room_type_id INT REFERENCES room_types(room_type_id),

            price NUMERIC,
            minimum_nights INT,
            number_of_reviews INT,
            last_review DATE,
            reviews_per_month NUMERIC,
            calculated_host_listings_count INT,
            availability_365 INT,
            number_of_reviews_ltm INT,
            license TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("Tables created successfully!\n")


# ---------------------------------------------------------------
# MAIN RUNNER (DEMO-FRIENDLY)
# ---------------------------------------------------------------

if __name__ == "__main__":
    print("\n==============================")
    print(" AIRBNB DATABASE RESET & SETUP")
    print("==============================\n")

    drop_tables()
    create_tables()

    print("Database schema successfully rebuilt.")