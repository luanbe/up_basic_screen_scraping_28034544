import pymysql.cursors
from pymysql import Error
from pymysql import connect
from .utils import remove_special_characters
import settings

db_host = settings.db_host
db_user = settings.db_user
db_password = settings.db_password
db_name = settings.db_name


def create_database(logger):
    # Create database and table if it is not exist
    try:
        with connect(
            host=db_host,
            user=db_user,
            password=db_password,
            cursorclass=pymysql.cursors.DictCursor
        ) as connection:
            create_db_query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
            with connection.cursor() as cursor:
                cursor.execute(create_db_query)
    except Error as e:
        raise ValueError(e)

def create_table(logger):
    # Create database and table if it is not exist
    try:
        with connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        ) as connection:
            create_projects_table_query = """
                CREATE TABLE projects(
                    project_id INT NOT NULL,
                    title VARCHAR(200),
                    blurb VARCHAR(500),
                    feature_image TEXT,
                    category VARCHAR(100),
                    category_id INT,
                    parent_category VARCHAR(100),
                    parent_category_id INT,
                    currency VARCHAR(5),
                    pledged INT,
                    goal INT,
                    backers INT,
                    day_to_go INT,
                    launched DATETIME,
                    deadline DATETIME,
                    location VARCHAR(200),
                    creator_id INT,
                    url VARCHAR(1000),
                    story LONGTEXT,
                    PRIMARY KEY (project_id),
                    FOREIGN KEY (creator_id) REFERENCES creators(creator_id)
                )
                """

            create_creators_table_query = """
                CREATE TABLE creators(
                    creator_id INT NOT NULL,
                    name VARCHAR(100),
                    verified_name VARCHAR(100),
                    slug VARCHAR(100),
                    location VARCHAR(5),
                    project INT,
                    backed_project INT,
                    join_date DATETIME,
                    biography TEXT,
                    crawl_status BOOLEAN DEFAULT FALSE,
                    active_projects BOOLEAN DEFAULT TRUE,
                    PRIMARY KEY (creator_id)
                )
                """
            create_updates_table_query = """
                CREATE TABLE updates(
                    update_id INT NOT NULL AUTO_INCREMENT,
                    title VARCHAR(200),
                    body TEXT,
                    comment_count INT,
                    like_count INT,
                    date DATETIME,
                    author VARCHAR(100),
                    author_role VARCHAR(50),
                    project_id INT,
                    PRIMARY KEY (update_id),
                    FOREIGN KEY (project_id) REFERENCES projects(project_id)
                )
                """
            create_comments_table_query = """
                CREATE TABLE comments(
                    comment_id INT NOT NULL AUTO_INCREMENT,
                    author VARCHAR(100),
                    body TEXT,
                    id VARCHAR(100),
                    parent_id VARCHAR(100),
                    comment_type VARCHAR(50),
                    project_id INT,   
                    PRIMARY KEY (comment_id),
                    FOREIGN KEY (project_id) REFERENCES projects(project_id)
                )
                """
            create_rewards_table_query = """
                CREATE TABLE rewards(
                    reward_id INT NOT NULL AUTO_INCREMENT,
                    title VARCHAR(200),
                    description TEXT,
                    reward_status BOOLEAN,
                    pledge_minimum VARCHAR(10),
                    ship_status BOOLEAN,
                    ship_to VARCHAR(50),
                    estimated_delivery DATETIME,
                    backers_count VARCHAR(10),
                    project_id INT,
                    PRIMARY KEY (reward_id),
                    FOREIGN KEY (project_id) REFERENCES projects(project_id)
                )
                """
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
            with connection.cursor() as cursor:
                cursor.execute(create_creators_table_query)
                cursor.execute(create_projects_table_query)
                cursor.execute(create_updates_table_query)
                cursor.execute(create_comments_table_query)
                cursor.execute(create_rewards_table_query)
                cursor.execute(create_crawl_status_table_query)
                connection.commit()
    except Error as e:
        logger.error(f'MYSQL error: {e}')


def insert_data(creator, project, updates, comments, rewards, logger):
    # Create database and table if it is not exist
    try:
        with connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        ) as connection:
            insert_creators_query = """
                INSERT INTO creators(
                    creator_id,
                    name,
                    verified_name,
                    slug,
                    location,
                    project,
                    backed_project,
                    join_date,
                    biography
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            insert_projects_query = """
                INSERT INTO projects(
                    project_id,
                    title,
                    blurb,
                    feature_image,
                    category,
                    category_id,
                    parent_category,
                    parent_category_id,
                    currency,
                    pledged,
                    goal,
                    backers,
                    day_to_go,
                    launched,
                    deadline,
                    location,
                    creator_id,
                    url,
                    story
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            insert_updates_query = """
                INSERT INTO updates(
                    title,
                    body,
                    comment_count,
                    like_count,
                    date,
                    author,
                    author_role,
                    project_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
            insert_comments_query = """
                INSERT INTO comments(
                    author,
                    body,
                    id,
                    parent_id,
                    comment_type,
                    project_id
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """
            insert_rewards_query = """
                INSERT INTO rewards(
                    title,
                    description,
                    reward_status,
                    pledge_minimum,
                    ship_status,
                    ship_to,
                    estimated_delivery,
                    backers_count,
                    project_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            with connection.cursor() as cursor:
                if creator:
                    list_creators = []
                    creator_id = creator['creator_id']
                    cursor.execute(f'SELECT creator_id FROM creators WHERE creator_id="{creator_id}"')
                    if not cursor.fetchall():
                        list_creators.append((
                            creator_id,
                            creator['name'],
                            creator['verified_name'],
                            creator['slug'],
                            creator['location'],
                            creator['project'],
                            creator['backed_project'],
                            creator['join_date'],
                            creator['biography']
                        ))
                    else:
                        logger.info(f'Creator ID {creator_id} is exist')
                    if list_creators:
                            cursor.executemany(insert_creators_query, list_creators)
                            connection.commit()
                            logger.info(f'Complete to insert ({len(list_creators)}) creators to database)')
                    else:
                        logger.info(f'None list creator to insert database')

                if project:
                    list_projects = []
                    project_id = project['project_id']

                    cursor.execute(f'SELECT project_id FROM projects WHERE project_id="{project_id}"')
                    if not cursor.fetchall():
                        if project['title']:
                            title = remove_special_characters(project['title'])
                        else:
                            title = None
                        
                        if project['blurb']:
                            blurb = remove_special_characters(str(project['blurb']))
                        else:
                            blurb = None

                        if project['story']:
                            story = remove_special_characters(project['story'])
                        else:
                            story = None
                        list_projects.append((
                            project_id,
                            title,
                            blurb,
                            project['feature_image'],
                            project['category'],
                            project['category_id'],
                            project['parent_category'],
                            project['parent_category_id'],
                            project['currency'],
                            project['pledged'],
                            project['goal'],
                            project['backers'],
                            project['day_to_go'],
                            project['launched'],
                            project['deadline'],
                            project['location'],
                            project['creator_id'],
                            project['url'],
                            story,
                        ))
                    else:
                        logger.info(f'Project ID {project_id} is exist')
                    if list_projects:
                            cursor.executemany(insert_projects_query, list_projects)
                            connection.commit()
                            logger.info(f'Complete to insert ({len(list_projects)}) projects to database)')
                    else:
                        logger.info(f'None list project to insert database')

                if updates:
                    list_updates = []
                    for up_date in updates:
                        project_id = up_date['project_id']
                        if up_date['title']:
                            title = remove_special_characters(up_date['title'])
                        else:
                            title = None
                        
                        if up_date['body']:
                            body = remove_special_characters(up_date['body'])
                        else:
                            body = None

                        if up_date['author']:
                            author = remove_special_characters(up_date['author'])
                        else:
                            author = None
                        cursor.execute(f'SELECT project_id FROM updates WHERE project_id="{project_id}" AND title="{title}"')
                        if not cursor.fetchall():
                            list_updates.append((
                                title,
                                body,
                                up_date['comment_count'],
                                up_date['like_count'],
                                up_date['date'],
                                author,
                                up_date['author_role'],
                                project_id,
                            ))
                        else:
                            logger.info(f'Updates with Project ID {project_id}  is exist')
                    if list_updates:
                        cursor.executemany(insert_updates_query, list_updates)
                        connection.commit()
                        logger.info(f'Complete to insert ({len(list_updates)}) updates to database)')
                    else:
                        logger.info(f'None list updates to insert database')

                if comments:
                    list_comments = []
                    for comment in comments:
                        project_id = comment['project_id']
                        if comment['body']:
                            body = remove_special_characters(comment['body'])
                        else:
                            body = None
                        author = remove_special_characters(comment['author'])
                        comment_id = comment['id']
                        cursor.execute(f'SELECT project_id FROM comments WHERE project_id="{project_id}" AND id="{comment_id}"')
                        if not cursor.fetchall():
                            list_comments.append((
                                author,
                                body,
                                comment_id,
                                comment['parent_id'],
                                comment['comment_type'],
                                project_id,
                            ))
                        else:
                            logger.info(f'Comment with Project ID {project_id} is exist')
                    if list_comments:  
                        cursor.executemany(insert_comments_query, list_comments)
                        connection.commit()
                        logger.info(f'Complete to insert ({len(list_comments)}) comments to database)')
                    else:
                        logger.info(f'None list comments to insert database')
                
                if rewards:
                    list_rewards = []
                    for reward in rewards:
                        project_id = reward['project_id']
                        if reward['title']:
                            title = remove_special_characters(reward['title'])
                        else:
                            title = None
                        
                        if reward['description']:
                            description = remove_special_characters(reward['description'])
                        else:
                            description = None
                            
                        if title:
                            cursor.execute(f'SELECT project_id FROM rewards WHERE project_id="{project_id}" AND title="{title}"')
                        elif description:
                            cursor.execute(f'SELECT project_id FROM rewards WHERE project_id="{project_id}" AND description="{description}"')
                        else:
                             cursor.execute(f'SELECT project_id FROM rewards WHERE project_id="{project_id}"')

                        if not cursor.fetchall():
                            list_rewards.append((
                                title,
                                description,
                                reward['reward_status'],
                                reward['pledge_minimum'],
                                reward['ship_status'],
                                reward['ship_to'],
                                reward['estimated_delivery'],
                                reward['backers_count'],
                                project_id,
                            ))
                        else:
                            logger.info(f'Rewards with Project ID {project_id} is exist')
                    if list_rewards:
                        cursor.executemany(insert_rewards_query, list_rewards)
                        connection.commit()
                        logger.info(f'Complete to insert ({len(list_rewards)}) rewards to database)')
                    else:
                        logger.info(f'None list rewards to insert database')

    except Error as e:
        logger.error(f'MYSQL error: {e}')

def fetch_all_crawler(crawler, logger):
    crawlers = None
    try:
        with connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        ) as connection:
            query = """
                    SELECT category_id, category_name, state, page_number, project_id, limit_status, crawl_id FROM crawl_status WHERE crawler = %s
                """
            with connection.cursor() as cursor:
                cursor.execute(query, crawler)
                crawlers = cursor.fetchall() 
    except Error as e:
        if logger:
            logger.error(f'MYSQL error: {e}')
        else:
            print(f'MYSQL error: {e}')
    
    return crawlers

def fetch_crawl(crawl_id, logger):
    crawler = None
    try:
        with connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        ) as connection:
            query = """
                    SELECT page_number, project_id, limit_status FROM crawl_status WHERE crawl_id = %s
                """
            with connection.cursor() as cursor:
                cursor.execute(query, crawl_id)
                crawler = cursor.fetchone() 
    except Error as e:
        if logger:
            logger.error(f'MYSQL error: {e}')
        else:
            print(f'MYSQL error: {e}')
    
    return crawler

def check_project_id(project_id, logger):
    project = None
    try:
        with connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        ) as connection:
            query = """
                    SELECT project_id from projects where project_id= %s
                """
            with connection.cursor() as cursor:
                cursor.execute(query, project_id)
                project = cursor.fetchone() 
    except Error as e:
        if logger:
            logger.error(f'MYSQL error: {e}')
        else:
            print(f'MYSQL error: {e}')
    
    return project

def update_crawl_status(data: tuple, logger):
    try:
        with connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        ) as connection:
            query = """
                UPDATE crawl_status SET page_number = %s , project_id = %s, limit_status = %s WHERE crawl_id = %s
                """
            with connection.cursor() as cursor:
                cursor.execute(query, data)
                connection.commit() 
    except Error as e:
        logger.error(f'MYSQL error: {e}')


def fetch_creators_not_crawl(logger):
    creators = None
    try:
        with connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        ) as connection:
            query = """
                    SELECT creator_id FROM creators WHERE crawl_status=False and active_projects=True
                """
            with connection.cursor() as cursor:
                cursor.execute(query)
                creators = cursor.fetchall()
    except Error as e:
        if logger:
            logger.error(f'MYSQL error: {e}')
        else:
            print(f'MYSQL error: {e}')
    
    return creators

def update_crawl_creator_status(creator_id, crawl_status, active_projects, logger):
    try:
        with connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        ) as connection:
            with connection.cursor() as cursor:
                query_update = """
                    UPDATE creators SET crawl_status = %s, active_projects = %s WHERE creator_id = %s
                """
                cursor.execute(query_update, (crawl_status, active_projects, creator_id))
                connection.commit() 

    except Error as e:
        logger.error(f'MYSQL error: {e}')