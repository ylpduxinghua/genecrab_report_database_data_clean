import sys
sys.path.append('/duxinghua/genecrab_report_database_data_clean')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info
from datetime import datetime
import pandas as pd
import numpy as np
import re


def main():
    conn = get_database_conn()
    sql = '''SELECT
                id,
                report_id,
                report_sample_id,
                num_exonic_bases_coverage,
                mub,
                `level`,
                rank
            FROM
                report_tmb_info
            WHERE
	            date( created_at ) >= '2019-06-01' 
	            AND date( created_at ) <= '2021-12-31';'''

    result = query_database(conn, sql)

    conn.close()

    # 创建存储数据的dataframe
    columns_list = ['report_id', 'report_sample_id', 'num_exonic_bases_coverage', 'mub', 'level',
        'rank']

    df = pd.DataFrame(data=None, columns=columns_list)

    # 存储数据
    for tuple in result:
        id, report_id, report_sample_id, num_exonic_bases_coverage, mub, level, rank = tuple
        df.loc[id, 'report_id'] = report_id
        df.loc[id, 'report_sample_id'] = report_sample_id
        df.loc[id, 'num_exonic_bases_coverage'] = num_exonic_bases_coverage
        df.loc[id, 'mub'] = mub
        df.loc[id, 'level'] = level
        df.loc[id, 'rank'] = rank


    df.to_csv('big_panel_tmb.tsv', sep='\t')
    # df = pd.read_csv(sys.argv[1], sep='\t', index_col=0)

    # tmb分位值 保留四位小数
    df.loc[:, 'rank'] = df.loc[:, 'rank'].apply(keep_four_digit)

    # level
    df.loc[:, 'level'] = df.loc[:, 'level'].apply(deal_level)

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
    big_panel_tmb_sql = '''create table big_panel_tmb_clean_final (
        id int(10) not null auto_increment,
        report_id int(10) not null comment "reports表id",
        report_sample_id int(10) not null comment "report_samples表id",
        num_exonic_bases_coverage int(11) not null comment "外显子覆盖度",
        mub decimal(6,2) default null comment 'tmb数值',
        level varchar(20) default null comment 'tmb水平',
        rank decimal(6,4) default null comment "tmb分位值",
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        primary key (id)
    )'''


    create_table(conn, 'big_panel_tmb_clean_final', big_panel_tmb_sql)
    conn.close()

    # 插入数据
    conn = get_database_conn()
    cur = conn.cursor()
    insert_sql = f'''insert into big_panel_tmb_clean_final (
                    report_id, report_sample_id, num_exonic_bases_coverage, mub, level,
                    rank, created_at, lot_number, lot_date_range) values (%s, %s, %s, %s, %s,
                                            %s, %s, %s, %s)'''
    insert_table_many_info(conn, cur, insert_sql, result_new)
    cur.close()
    conn.close()


def keep_four_digit(ele):
    try:
        if np.isnan(ele):
            pass
    except:
        ele = "%.4f" % (float(ele))
    return ele


def deal_level(ele):
    mapping_dict = {'High':'高',
                    'Extra High': '高',
                    'Low': '低',
                    'Moderate': '中'}
    return mapping_dict[ele]
    

if __name__ == '__main__':
    main()