import sys
sys.path.append('/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗脚本')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info
from datetime import datetime
import pandas as pd
import numpy as np
import re


def search_ele(ele):
    regex = r'^c\.(\d+)[A-Z]>[A-Z]$'
    regex2 = r'c\.(\d+)[-+](\d+)[A-Z]>[A-Z]$'
    regex3_1 = r'c\.(\d+)[-+](\d+)_(\d+)delins[A-Z]+$' # c.3867-1_3867delinsTT
    regex3_2 = r'c\.(\d+)_(\d+)[-+](\d+)delins[A-Z]+$' # c.3866_3867-1delinsTT
    regex3_3 = r'c\.(\d+)[-+](\d+)_(\d+)[-+](\d+)delins[A-Z]+$' # c.3866_3867-1delinsTT
    regex3_4 = r'c\.(\d+)[-+](\d+)delins[A-Z]+$'    # c.24-79delinsAT
    regex3_5 = r'c\.(\d+)delins[A-Z]+$'    # c.2479delinsAT
    regex4 = r'c\.(\d+)_(\d+)delins[A-Z]+$'
    regex5 = r'c\.(\d+)_(\d+)ins[A-Z]+$'
    match = re.search(regex, ele)
    match2 = re.search(regex2, ele)
    match3_1 = re.search(regex3_1, ele)
    match3_2 = re.search(regex3_2, ele)
    match3_3 = re.search(regex3_3, ele)
    match3_4 = re.search(regex3_4, ele)
    match3_5 = re.search(regex3_5, ele)
    match4 = re.search(regex4, ele)
    match5 = re.search(regex5, ele)
    if match or match4 or match5 or match2 or match3_1 or match3_2 or match3_3 or match3_4 or match3_5:
        return 0
    else:
        return ele


def main():
    out_file = '/Users/duxinghua/Desktop/out_c_point.tsv'
    conn = get_database_conn()
    sql = '''select DISTINCT mutation_c from report_mutations where selected = 1;'''
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