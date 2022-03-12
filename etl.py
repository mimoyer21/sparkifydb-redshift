import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import sqlalchemy as sa
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy import orm as sa_orm


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

def load_staging_tables(cur, conn):    
    for query in copy_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
        except psycopg2.InternalError as e:
            print(e)
            error_query = """
                select substring(err_reason,1,48), * 
                from stl_load_errors
                order by starttime desc
                limit 1;
            """
            cur.execute(error_query)
            conn.commit()
            for row in cur:
                print(row)
            

def insert_tables(cur, conn):
    try:
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)
        print('\n==========\n' + query + '\n==========\n')

def create_er_diagram(er_diagram_file_name='er_diagram.png'):
    """
    - Creates an entity relationship (ER) diagram in a png file to visually display the tables
    - of the database to enable easy visual understanding of the db. Saves the png file in the folder from which etl.py 
    - (this script) is run.
    """
    # build the sqlalchemy URL
    try:
        url = URL.create(
            drivername='redshift+redshift_connector', # indicate redshift_connector driver and dialect will be used
            host=config.get('CLUSTER','HOST'), # Amazon Redshift host
            port=config.get('CLUSTER','DB_PORT'), # Amazon Redshift port
            database=config.get('CLUSTER','DB_NAME'), # Amazon Redshift database
            username=config.get('CLUSTER','DB_USER'), # Amazon Redshift username
            password=config.get('CLUSTER','DB_PASSWORD') # Amazon Redshift password
        )
    except AttributeError as e:
        print(e)
        print("Note: if you have sqlalchemy installed instead of sqlalchemy-redshift, uninstall sqlalchemy and install sqlalchemy-redshift for this call to work")

    engine = sa.create_engine(url)

    Session = sa_orm.sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    # Define Session-based Metadata
    metadata = sa.MetaData(bind=session.bind)
    
    # Create the ERD graph and save to file
    graph = create_schema_graph(metadata=metadata)
    graph.write_png(er_diagram_file_name)
    print(f'ER diagram created as {er_diagram_file_name}')

def main():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    create_er_diagram('sparkify_data_erd.png')

    conn.close()


if __name__ == "__main__":
    main()