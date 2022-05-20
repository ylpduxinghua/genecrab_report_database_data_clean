import sys
sys.path.append('/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗脚本')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info
from datetime import datetime
import pandas as pd
import numpy as np
import re


def search_ele(ele):
    regex = r'^p\.[A-Z](\d+)[A-Z]$'
    regex2 = r'p\.[A-Z](\d+)\*$'
    regex3 = r'p\.[A-Z](\d+)[A-Z]fs\*(\d+)$'
    regex4 = r'p\.[A-Z](\d+)_[A-Z](\d+)delins[A-Z]+$'
    regex5 = r'p\.[A-Z](\d+)_[A-Z](\d+)ins[A-Z]+$'
    match = re.search(regex, ele)
    match2 = re.search(regex2, ele)
    match3 = re.search(regex3, ele)
    match4 = re.search(regex4, ele)
    match5 = re.search(regex5, ele)
    if match or match2 or match3 or match4 or match5:
        return 0
    else:
        return ele


def main():
    out_file = '/Users/duxinghua/Desktop/out_p_point.tsv'
    conn = get_database_conn()
    sql = '''select DISTINCT mutation_p from report_mutations where selected = 1;'''
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