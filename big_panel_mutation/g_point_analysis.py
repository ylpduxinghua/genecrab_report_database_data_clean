import sys
sys.path.append('/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗脚本')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info
from datetime import datetime
import pandas as pd
import numpy as np
import re


def search_ele(ele):
    if ele == None:
        return ele
    if 'chr' in ele:
        ele = ele.split(':')[1]

    regex = r'^g\.(\d+)[A-Z]>[A-Z]$'
    # regex2 = r'g\.(\d+)[-+](\d+)[A-Z]>[A-Z]$'
    # regex3_1 = r'g\.(\d+)[-+](\d+)_(\d+)delins[A-Z]+$' # c.3867-1_3867delinsTT
    # regex3_2 = r'g\.(\d+)_(\d+)[-+](\d+)delins[A-Z]+$' # c.3866_3867-1delinsTT
    # regex3_3 = r'g\.(\d+)[-+](\d+)_(\d+)[-+](\d+)delins[A-Z]+$' # c.3866_3867-1delinsTT
    # regex3_4 = r'g\.(\d+)[-+](\d+)delins[A-Z]+$'    # c.24-79delinsAT
    regex3_5 = r'g\.(\d+)delins[A-Z]+$'    # g.2479delinsAT
    regex4 = r'g\.(\d+)_(\d+)delins[A-Z]+$'
    regex5 = r'g\.(\d+)_(\d+)ins[A-Z]+$'
    match = re.search(regex, ele)
    # match2 = re.search(regex2, ele)
    # match3_1 = re.search(regex3_1, ele)
    # match3_2 = re.search(regex3_2, ele)
    # match3_3 = re.search(regex3_3, ele)
    # match3_4 = re.search(regex3_4, ele)
    match3_5 = re.search(regex3_5, ele)
    match4 = re.search(regex4, ele)
    match5 = re.search(regex5, ele)
    if match or match4 or match5 or match3_5:
        return 0
    else:
        return ele


def main():
    out_file = '/Users/duxinghua/Desktop/out_g_point.tsv'
    conn = get_database_conn()
    sql = '''SELECT distinct
                mutation_g_reviewed
            FROM
                report_mutations 
            WHERE
                selected = 1;'''
    result = query_database(conn, sql)
    conn.close()
    result_list = []
    for tuple in result:
        ele = search_ele(tuple[0])
        if ele:
            result_list.append(ele)

    with open(out_file, 'w') as f:
        f.write('\n'.join(result_list))



if __name__ == '__main__':
    main()