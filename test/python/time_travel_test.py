import duckdb

def test_time_travel():
    conn = duckdb.connect('');
    conn.execute("SELECT time_travel('Sam') as value;");
    res = conn.fetchall()
    assert(res[0][0] == "Time_travel Sam 🐥");