from sqlalchemy import create_engine
import pandas as pd

#THIS FUNCTION GET THE DATA FOR THE DATABASE FROM ZABBIX AND SHOWS IN ONE DATAFRAME.
def get_data_zabbix ():
    
    engine = create_engine("postgresql+psycopg2://zabbix:y0fc2025.@192.168.20.190:5432/zabbix")

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


    """
    
    }


    #EXECUTE THE QUERIES 
    with engine.connect() as conn:
        df_hosts = pd.read_sql(queries["disk_problem"], conn)
            
        print(df_hosts)

    return df_hosts


def send_data_csv (df):
    #GUARDADO DE DATOS EN UN CSV (CAMBIAR NOMBRE DEL ARCHIVO .CSV)
    df.to_csv(r"D:\datos_prt\datos_guardados\problem_w_disk.csv", index=False)



#===== CALLED OF THE FUNCTIONS =======
df_host = get_data_zabbix()
#send_data_csv(df_host)
