import sys
sys.path.append('/duxinghua/genecrab_report_database_data_clean')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info
from datetime import datetime
import pandas as pd
import numpy as np
import re


def main():
    # 获取数据库信息
    conn = get_database_conn()
    sql = '''SELECT
                id,
                report_id,
                report_sample_id,
                report_mutation_id,
                frequency,
                breakpoint1,
                breakpoint2,
                breakpoint1_support_reads,
                breakpoint2_support_reads,
                support_reads 
            FROM
                report_mutation_fusion_desc 
            WHERE
                report_mutation_id IN (
                SELECT DISTINCT
                    id 
                FROM
                    report_mutations m 
                WHERE
                    data_type = 'FUSION' 
                    AND selected = 1 UNION
                SELECT DISTINCT
                    id 
                FROM
                    small_panel_patient_positive_results 
                WHERE
                    data_type = 'FUSION' 
                    AND selected = 1 
                    AND frequency <> '-' 
                );'''
    result = query_database(conn, sql)

    conn.close()


    # 创建存储数据的dataframe
    columns_list = ['report_id', 'report_sample_id', 'report_mutation_id', 'frequency', 'breakpoint1',
                    'breakpoint2', 'breakpoint1_support_reads', 'breakpoint2_support_reads', 'spanning_reads']

    df = pd.DataFrame(data=None, columns=columns_list)
    # 存储数据
    for tuple in result:
        id, report_id, report_sample_id, report_mutation_id, frequency,\
        breakpoint1, breakpoint2, breakpoint1_support_reads, breakpoint2_support_reads, spanning_reads = tuple
        df.loc[id, :] = [report_id, report_sample_id, report_mutation_id, frequency,\
                        breakpoint1, breakpoint2, breakpoint1_support_reads, breakpoint2_support_reads, spanning_reads]

    # frequency
    df.loc[:, 'frequency'] =  df.loc[:, 'frequency'].apply(deal_frequency)
    # spanning reads
    df.loc[df.loc[:, 'spanning_reads'] == '', 'spanning_reads'] = np.nan


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

    # 创建小panel突变表
    conn = get_database_conn()
    all_panel_fusion_sql = '''create table all_panel_fusion_clean_final(
        id int(10) not null auto_increment,
        report_id int(10) not null comment 'reports表id',
        report_sample_id int(10) not null comment 'report_samples表id',
        report_mutation_id int(10) not null comment "report_mutations表id/small_panel_patient_positive_results表id",
        frequency decimal(6,2) default null comment "变异频率",
        breakpoint1 int(11) default null comment '断点1位置',
        breakpoint2 int(11) default null comment '断点2位置',
        breakpoint1_support_reads int(11) default null comment '断点1支持的reads数',
        breakpoint2_support_reads int(11) default null comment '断点2支持的reads数',
        spanning_reads int(11) default null comment '跨越reads数',
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        primary key (id)
    )'''
    create_table(conn, 'all_panel_fusion_clean_final', all_panel_fusion_sql)
    conn.close()

    # 插入数据
    conn = get_database_conn()
    cur = conn.cursor()
    insert_sql = f'''insert into all_panel_fusion_clean_final (
                    report_id, report_sample_id, report_mutation_id, frequency, breakpoint1,
                    breakpoint2, breakpoint1_support_reads, breakpoint2_support_reads, spanning_reads, created_at,
                    lot_number, lot_date_range) values (%s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s)'''
    insert_table_many_info(conn, cur, insert_sql, result_new)
    cur.close()
    conn.close()


def deal_frequency(ele):
    try:
        if np.isnan(ele):
            pass
    except:
        if '%' in ele:
            ele = ele.split('%')[0]
    return ele


if __name__ == '__main__':
    main()