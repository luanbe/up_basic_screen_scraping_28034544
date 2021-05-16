import pymysql.cursors
from pymysql import Error, DataError
from pymysql import connect


db_host = '207.148.66.216'
db_user = 'support_4'
db_password = 'Zipphong18091987@@'
db_name = 'kickstarter_com'


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
        print(e)

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
                    story TEXT,
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
            with connection.cursor() as cursor:
                cursor.execute(create_creators_table_query)
                cursor.execute(create_projects_table_query)
                cursor.execute(create_updates_table_query)
                cursor.execute(create_comments_table_query)
                cursor.execute(create_rewards_table_query)
                connection.commit()
    except Error as e:
        print(e)


def insert_data(creators, projects, updates, comments, rewards, logger):
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
                if creators:
                    list_creators = []
                    for creator in creators:
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

                if projects:
                    list_projects = []
                    for project in projects:
                        project_id = project['project_id']

                        cursor.execute(f'SELECT project_id FROM projects WHERE project_id="{project_id}"')
                        if not cursor.fetchall():
                            list_projects.append((
                                project_id,
                                project['title'],
                                project['blurb'],
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
                                project['story'],
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
                        title = up_date['title']
                        cursor.execute(f'SELECT project_id FROM updates WHERE project_id="{project_id}" AND title="{title}"')
                        if not cursor.fetchall():
                            list_updates.append((
                                title,
                                up_date['body'],
                                up_date['comment_count'],
                                up_date['like_count'],
                                up_date['date'],
                                up_date['author'],
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
                        body = comment['body']
                        author = comment['author']
                        cursor.execute(f'SELECT project_id FROM comments WHERE project_id="{project_id}" AND body="{author}"')
                        if not cursor.fetchall():
                            list_comments.append((
                                author,
                                body,
                                comment['id'],
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
                        title = reward['title']
                        pledge_minimum = reward['pledge_minimum']
                        cursor.execute(f'SELECT project_id FROM rewards WHERE project_id="{project_id}" AND title="{pledge_minimum}"')
                        if not cursor.fetchall():
                            list_rewards.append((
                                title,
                                reward['description'],
                                reward['reward_status'],
                                pledge_minimum,
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
        print(e)
