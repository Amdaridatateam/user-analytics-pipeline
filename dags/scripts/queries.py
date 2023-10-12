create_query = """
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
        invoiceNo VARCHAR(10),
        stockCode VARCHAR(20),
        description VARCHAR(1000),
        quantity NUMERIC,
        invoiceDate TIMESTAMP,
        unitPrice NUMERIC(8, 3),
        customerID VARCHAR(20),
        country VARCHAR(20)
    );
"""