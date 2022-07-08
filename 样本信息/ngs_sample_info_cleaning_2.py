'''该脚本根据genecrab_report_static库中的mgs_sample_information_79数据表的内容修改ngs_sample_information_clean_final表
该脚本已经废弃，相关逻辑加入ngs_sample_info_cleaning.py脚本中
'''
import sys
sys.path.append('/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗脚本')
from mysql_module import get_database_conn, my_database_conn, query_database, update_table
from datetime import datetime
import pandas as pd
import numpy as np
import re


def main():
    # 获取ngs_sample_information_clean_final表和mgs_sample_information_79表内容
    conn = get_database_conn()
    sql = '''SELECT
                source_id,
                body_part_name,
                body_part_name_new 
            FROM
                ngs_sample_information_79;'''

    result = query_database(conn, sql)

    conn.close()

    # 将数据存入dict
    conn = get_database_conn()
    cur = conn.cursor()
    mapping_dict = {'无法判断': 'null', 'NULL': 'null'}
    for tuple in result:
        source_id, body_part_name, body_part_name_new = tuple
        if body_part_name_new in mapping_dict:
            body_part_name_new = mapping_dict[body_part_name_new]
        update_sql = f'''update ngs_sample_information_clean_final
                        set body_part_name = '{body_part_name_new}'
                        where id = {source_id} '''
        print(update_sql)
        update_sql = update_sql.replace("'null'", "null")
        update_table(conn, cur, update_sql)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()