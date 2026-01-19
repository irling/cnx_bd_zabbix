from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import os

# LOAD VAR FOR .ENV
load_dotenv()

# LOCAL VAR 
DB_USER = os.getenv("ZBX_DB_USER")
DB_PASS = os.getenv("ZBX_DB_PASS")
DB_HOST = os.getenv("ZBX_DB_HOST")
DB_PORT = os.getenv("ZBX_DB_PORT")
DB_NAME = os.getenv("ZBX_DB_NAME")

#THIS FUNCTION GET THE DATA FOR THE DATABASE FROM ZABBIX AND SHOWS IN ONE DATAFRAME.
def get_data_zabbix ():
    
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    #QUERIES FOR THE DATABASE OF ZABBIX
    queries = {
    "problem": """
        SELECT *
        FROM problem_tag
    """,

    "hstgrp": """
        SELECT *
        FROM hstgrp
    """,

    "get_name_region": """
        SELECT
            h.name AS host_name,
            g.name AS group_name
        FROM hosts h
        JOIN hosts_groups hg ON h.hostid = hg.hostid
        JOIN hstgrp g ON hg.groupid = g.groupid
        WHERE g.name ILIKE ANY (ARRAY[
            '%%AN-TX%%',
            '%%AN-AX%%',
            '%%LL-AX%%',
            '%%LL-TX%%',
            '%%AR-AX%%',
            '%%AR-TX%%',
            '%%SM-AX%%',
            '%%SM-TX%%'
        ])
        ORDER BY h.name;
    """,
    
    "get_all_data": """
        SELECT
        h.name AS host_name,
        g.name AS group_name,
        t.description AS problem_description,
        to_timestamp(p.clock) AS problem_time
    FROM problem p
    JOIN triggers t ON p.objectid = t.triggerid
    JOIN functions f ON t.triggerid = f.triggerid
    JOIN items i ON f.itemid = i.itemid
    JOIN hosts h ON i.hostid = h.hostid
    JOIN hosts_groups hg ON h.hostid = hg.hostid
    JOIN hstgrp g ON hg.groupid = g.groupid
    WHERE g.name ILIKE ANY (ARRAY[
        '%%AN-TX%%',
        '%%AN-AX%%',
        '%%LL-AX%%',
        '%%LL-TX%%',
        '%%AR-AX%%',
        '%%AR-TX%%',
        '%%SM-AX%%',
        '%%SM-TX%%'
    ])
    AND p.clock >= extract(epoch FROM now() - interval '24 hours')
    ORDER BY p.clock DESC;

    """,
    
    "disk_problem": """
            SELECT
                h.name AS host_name,
                regexp_replace(t.description, '.*FS \\[([^\\]]+)\\].*', '\1') AS filesystem,
                t.description AS trigger_description,
                to_timestamp(p.clock) AS problem_time
            FROM problem p
            JOIN triggers t ON p.objectid = t.triggerid
            JOIN functions f ON t.triggerid = f.triggerid
            JOIN items i ON f.itemid = i.itemid
            JOIN hosts h ON i.hostid = h.hostid
            WHERE
            (
                t.description ILIKE '%%FS %%Space is low%%'
                OR t.description ILIKE '%%FS %%Space is critically low%%'
                OR t.description ILIKE '%%Free space is low%%'
                OR t.description ILIKE '%%Free space is critically low%%'
            )
            ORDER BY p.clock DESC;


        """,
        "disk_problem_w_time": """
        
            SELECT
            h.name AS host_name,
            regexp_replace(t.description, '.*FS \\[([^\\]]+)\\].*', '\1') AS filesystem,
            hu_pused.value AS percent_used,
            ROUND(hu_total.value / 1024 / 1024 / 1024, 1) AS total_gb,
            to_timestamp(p.clock) AS problem_time
        FROM problem p
        JOIN triggers t ON p.objectid = t.triggerid
        JOIN functions f ON t.triggerid = f.triggerid
        JOIN items i ON f.itemid = i.itemid
        JOIN hosts h ON i.hostid = h.hostid
        LEFT JOIN items i_pused
            ON i_pused.hostid = h.hostid
            AND i_pused.key_ ILIKE 'vfs.fs.size%pused%'
        LEFT JOIN history_uint hu_pused
            ON hu_pused.itemid = i_pused.itemid
        LEFT JOIN items i_total
            ON i_total.hostid = h.hostid
            AND i_total.key_ ILIKE 'vfs.fs.size%total%'
        LEFT JOIN history_uint hu_total
            ON hu_total.itemid = i_total.itemid
        WHERE
        (
            t.description ILIKE '%%FS %%Space is low%%'
            OR t.description ILIKE '%%FS %%Space is critically low%%'
            OR t.description ILIKE '%%Free space is low%%'
            OR t.description ILIKE '%%Free space is critically low%%'
        )
        ORDER BY p.clock DESC;


    """
    
    }


    #EXECUTE THE QUERIES 
    with engine.connect() as conn:
        df_hosts = pd.read_sql(text(queries["disk_problem"]), conn)
            
        print(df_hosts)

    return df_hosts


def send_data_csv (df):
    #GUARDADO DE DATOS EN UN CSV (CAMBIAR NOMBRE DEL ARCHIVO .CSV)
    df.to_csv(r"D:\datos_prt\datos_guardados\problem_w_disk_02.csv", index=False)



#===== CALLED OF THE FUNCTIONS =======
df_host = get_data_zabbix()
send_data_csv(df_host)
