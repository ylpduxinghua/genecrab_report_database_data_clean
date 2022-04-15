import pymysql


# 连接数据库
def get_database_conn():
    conn = pymysql.connect(host='172.16.20.43',
            port = 3306,
            user = 'duxhua',
            passwd = 'Genecast@123',
            db = 'genecrab_report_static')
    return conn


def my_database_conn():
    conn = pymysql.connect(host='localhost',
            port = 3306,
            user = 'root',
            passwd = 'password',
            db = 'mysql_learn')
    return conn



def query_database(conn, sql):
    # 创建游标
    cur = conn.cursor()

    try:
        cur.execute(sql)
        results = cur.fetchall()
        # for row in results:
        #     row = [str(ele) for el e in row]
        #     print(','.join(row))
    except Exception as e:
        raise e

    # 获取前三行
    # ret2 = cur.fetchmany(2)
    # print(ret2)

    # 获取第一行
    # ret3 = cur.fetchone()
    # print(ret3)

    # 关闭指针对象
    cur.close()

    # 关闭数据库连接
    # conn.close()

    return results


def create_table(conn, table_name, sql):
        # 创建游标对象
        cur = conn.cursor()
        # 使用execute()方法执行SQL， 如果表存在则删除
        cur.execute(f'DROP TABLE IF EXISTS {table_name}')
        # 创建表
        cur.execute(sql)
        # 关闭指针对象
        cur.close()


def insert_table_info(conn, cur, sql):
    try:
        # 执行sql语句
        cur.execute(sql)
        # 提交到数据库执行
        conn.commit()
    except Exception as e:
        # 如果发生错误则回滚
        conn.rollback()
        raise e

def insert_table_many_info(conn, cur, sql, args):
    try:
        # 执行sql语句
        cur.executemany(sql, args)
        # 提交到数据库执行
        conn.commit()
    except Exception as e:
        # 如果发生错误则回滚
        conn.rollback()
        raise e



def get_hospital_mapping_dict():
    conn = get_database_conn()
    sql = '''SELECT
                * 
            FROM
                mapping_hospital_to_std;'''

    result = query_database(conn, sql)
    conn.close()
    
    hospital_mapping_dict = {}
    for tuple in result:
        hospital_mapping_dict[tuple[1]] = tuple[2]

    return hospital_mapping_dict











