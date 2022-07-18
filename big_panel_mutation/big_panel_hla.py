import sys
sys.path.append('/duxinghua/genecrab_report_database_data_clean')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info
from datetime import datetime
import pandas as pd
import numpy as np
import re


def main():
    conn = get_database_conn()
    sql = '''SELECT DISTINCT
                x.id, x.report_id, x.report_sample_id, x.gene, x.allele, x.zygosity, x.super_type
            FROM
                (
                SELECT
                    h.* 
                FROM
                    report_hla_supertypes h
                    inner JOIN report_samples r ON h.report_sample_id = r.id 
                WHERE
                    h.report_id IN ( SELECT report_id FROM report_hla_supertypes GROUP BY report_id HAVING count( 1 )> 3 ) 
                    AND r.sample_attribute = 'BLOOD' 
                    AND h.selected = 1 UNION ALL
                SELECT
                    h.* 
                FROM
                    report_hla_supertypes h
                WHERE
                    h.report_id IN ( SELECT report_id FROM report_hla_supertypes GROUP BY report_id HAVING count( 1 )= 3 ) 
                    AND h.selected = 1 
                ) x;'''

    result = query_database(conn, sql)

    conn.close()

    # 创建存储数据的dataframe
    columns_list = ['report_id', 'report_sample_id', 'gene', 'allele', 'zygosity', 'super_type']

    df = pd.DataFrame(data=None, columns=columns_list)

    # 存储数据
    for tuple in result:
        id, report_id, report_sample_id, gene, allele, zygosity, super_type = tuple
        df.loc[id, 'report_id'] = report_id
        df.loc[id, 'report_sample_id'] = report_sample_id
        df.loc[id, 'gene'] = gene
        df.loc[id, 'allele'] = allele
        df.loc[id, 'zygosity'] = zygosity
        df.loc[id, 'super_type'] = super_type

    df.to_csv('big_panel_hla.tsv', sep='\t')
    # df = pd.read_csv(sys.argv[1], sep='\t', index_col=0)

    # zygosity
    df.loc[:, 'zygosity'] = df.loc[:, 'zygosity'].map({1:'纯合', 0:'杂合'})

    # 建表时间 清洗批次信息 清洗数据的时间范围
    df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['lot_number'] = 0
    df['lot_date_range'] = '>=2019-06-01,<=2021-12-31'

    # nan转化为'None'
    df = df.where(~df.isna(), None)

    # dataframe转化为list
    result = df.values.tolist()

    # 数值转化为str
    result_new = []
    for tmp in result:
        result_new.append([ele if ele is None else str(ele) for ele in tmp])

    # 创建大pamel tmb表
    conn = get_database_conn()
    big_panel_hla_sql = '''create table big_panel_hla_clean_final (
        id int(10) not null auto_increment,
        report_id int(10) not null comment "reports表id",
        report_sample_id int(10) not null comment "report_samples表id",
        gene varchar(255) default null comment "基因",
        allele varchar(255) default null comment "等位基因",
        zygosity varchar(20) default null comment "纯杂合",
        super_type varchar(255) default null comment "超型",
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        primary key (id)
    )'''


    create_table(conn, 'big_panel_hla_clean_final', big_panel_hla_sql)
    conn.close()

    # 插入数据
    conn = get_database_conn()
    cur = conn.cursor()
    insert_sql = f'''insert into big_panel_hla_clean_final (
                    report_id, report_sample_id, gene, allele, zygosity,
                    super_type, created_at, lot_number, lot_date_range) values (%s, %s, %s, %s, %s,
                                            %s, %s, %s, %s)'''
    insert_table_many_info(conn, cur, insert_sql, result_new)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()