import pymysql.cursors
import pandas as pd
import settings

from pymysql import Error
from pymysql import connect

db_host = settings.db_host
db_user = settings.db_user
db_password = settings.db_password
db_name = settings.db_name

df = pd.read_excel('./crawl_status.xlsx')
data_excel = []
for ind in df.index:
    if pd.isna(df['State'][ind]) is True:
        state = None
    else:
        state = df['State'][ind]
    
    if pd.isna(df['project_id'][ind]) is True:
        project_id = None
    else:
        project_id = df['project_id'][ind]
    
    if pd.isna(df['VPS'][ind]) is True:
        vps = None
    else:
        vps = df['VPS'][ind]

    if df['Limit Status'][ind] == False:
        limit_status = 0
    else:
        limit_status = 1
    data_excel.append((
        df['ID'][ind],
        df['Parent ID'][ind],
        df['Category'][ind],
        state,
        df['Page No.'][ind],
        project_id,
        limit_status,
        vps,
        ))

try:
    with connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    ) as connection:
        create_crawl_status_table_query = """
                CREATE TABLE crawl_status(
                    crawl_id INT NOT NULL AUTO_INCREMENT,
                    category_id INT,
                    parent_category_id INT,
                    category_name VARCHAR(100),
                    state VARCHAR(50),
                    page_number INT,
                    project_id INT,
                    limit_status BOOLEAN,
                    crawler VARCHAR(50),
                    PRIMARY KEY (crawl_id)
                )
                """
        insert_crawl_status_query = """
                INSERT INTO crawl_status(
                    category_id,
                    parent_category_id,
                    category_name,
                    state,
                    page_number,
                    project_id,
                    limit_status,
                    crawler
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """

        list_data_query = """
            SELECT category_id, category_name, state, page_number, project_id, limit_status, crawler, crawl_id FROM crawl_status WHERE crawler=%s
        """

        update_data_query = """
            UPDATE crawl_status SET page_number = %s , project_id = %s, limit_status = %s WHERE crawl_id = %s
        """

        fetch_data_by_id = """
            SELECT page_number, project_id, limit_status FROM crawl_status WHERE crawl_id = %s
        """
        with connection.cursor() as cursor:
            # Create table
            # cursor.execute(create_crawl_status_table_query)
            # connection.commit()
           
            # Add data
            cursor.executemany(insert_crawl_status_query, data_excel)
            connection.commit()

            # List data
            # cursor.execute(list_data_query, 'TOR_1')
            # print(cursor.fetchall())

            # List data
            # cursor.execute(fetch_data_by_id, '75')
            # print(cursor.fetchone())

            # Update data
            # cursor.execute(update_data_query, (20, 'new', True, 75))
            # connection.commit()


except Error as e:
    print(f'Noi dung show error: {e}')