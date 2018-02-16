using ODBC

function read_excel_db(dsn_str::AbstractString)
    dns = ODBC.DSN(dsn_str)
    ODBC.query(dsn, "SELECT * FROM entity_class")
end
