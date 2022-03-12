import configparser
import psycopg2

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

def print_db_tables_and_data(cur, conn):
    """For testing purposes: prints names of all created tables to console and 5 rows from each table."""
    
    query = "SELECT DISTINCT tablename FROM PG_TABLE_DEF WHERE schemaname = 'public';"
    cur.execute(query)
    conn.commit()
    print("\n=============\nTables in database:\n")
    table_names = []
    for table in cur:
        table_names.append(table[0])
        print(table[0])
    print("\n=============")
    
    print("\n=============\nExample data:")
    example_data_query = "SELECT * FROM {} LIMIT 5;"
    for table_name in table_names:
        cur.execute(example_data_query.format(table_name))
        conn.commit()
        print(f"\n=============\n{table_name}:\n")
        for row in cur:
            print(row)
        
def main():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
        
#     test_query1 = """
#             with recency_ranks as (
#                 select userId as user_id
#                     , firstName as first_name
#                     , lastName as last_name
#                     , gender
#                     , level
#                     , row_number() over (partition by userId order by ts desc) as recency_rank
#                 from staging_events
#                 where userId is not null
#             )
#             select user_id
#                 , first_name
#                 , last_name
#                 , gender
#                 , level
#             from recency_ranks
#             where recency_rank = 1
#             limit 10
#             ;
#     """
#     test_query2 = "select count(*) from staging_events where userId is not null or itemInSession is not null;"
#     test_query3 = "select count(*) from staging_songs where num_songs is not null;"
#     test_query4 = "SELECT * FROM PG_TABLE_DEF WHERE tablename = 'staging_events';"
    
#     # can choose which test query is run here
#     cur.execute(test_query2)
#     conn.commit()
    
#     for row in cur:
#         print(row)
    
    print_db_tables_and_data(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()