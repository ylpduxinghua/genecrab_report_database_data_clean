import sys
sys.path.append('/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗脚本')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info
from datetime import datetime
import pandas as pd
import numpy as np
import re


def main():
    conn = get_database_conn()
    sql = '''SELECT
                d.id,
                d.report_id,
                d.report_sample_id,
                d.report_mutation_id,
                d.frequency,
                q.qc_title,
                q.qc_value
            FROM
                report_mutations m
                INNER JOIN report_mutation_desc d ON m.id = d.report_mutation_id
                INNER JOIN report_variant_qc q ON d.id = q.report_mutation_desc_id 
            WHERE
                m.selected = 1 
                AND m.data_type = 'SNV' 
                AND q.qc_title IN ( '突变总的支持reads数', '深度', '正链支持reads数', '负链支持reads数' );'''

    result = query_database(conn, sql)

    conn.close()

    # 创建存储数据的dataframe
    columns_list = ['report_id', 'report_sample_id', 'report_mutation_id', 'frequency', 'read_depth',
        'allele_depth', 'forward_reads', 'reverse_reads']

    df = pd.DataFrame(data=None, columns=columns_list)

    # 存储数据
    mapping_dict = {
        '突变总的支持reads数':'allele_depth',
        '深度':'read_depth',
        '正链支持reads数':'forward_reads',
        '负链支持reads数':'reverse_reads'
    }
    for tuple in result:
        id, report_id, report_sample_id, report_mutation_id, frequency, qc_title, qc_value = tuple
        df.loc[id, 'report_id'] = report_id
        df.loc[id, 'report_sample_id'] = report_sample_id
        df.loc[id, 'report_mutation_id'] = report_mutation_id
        df.loc[id, 'frequency'] = frequency
        df.loc[id, mapping_dict[qc_title]] = qc_value

    df.to_csv('big_panel_snv_qc.tsv', sep='\t')
    # df = pd.read_csv(sys.argv[1], sep='\t', index_col=0)
    
    df.loc[:, 'frequency'] = df.loc[:, 'frequency'].apply(deal_frequency)

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


    # 创建大panel SNV QC表
    conn = get_database_conn()
    big_panel_snv_qc_sql = '''create table big_panel_snv_qc_clean_final (
        id int(10) not null auto_increment,
        report_id int(10) not null comment "reports表id",
        report_sample_id int(10) not null comment "report_samples表id",
        report_mutation_id int(10) not null comment "report_mutations表id",
        frequency decimal(6,2) default null comment '变异频率',
        read_depth int(11) default null comment "测序深度",
        allele_depth int(11) default null comment "突变总reads数",
        forward_reads int(11) default null comment "正链支持reads数",
        reverse_reads int(11) default null comment "负链支持reads数",
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        primary key (id)
    )'''


    create_table(conn, 'big_panel_snv_qc_clean_final', big_panel_snv_qc_sql)
    conn.close()

    # 插入数据
    conn = get_database_conn()
    cur = conn.cursor()
    insert_sql = f'''insert into big_panel_snv_qc_clean_final (
                    report_id, report_sample_id, report_mutation_id, frequency, read_depth,
                    allele_depth, forward_reads, reverse_reads, created_at, lot_number,
                    lot_date_range) values (%s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s)'''
    insert_table_many_info(conn, cur, insert_sql, result_new)
    cur.close()
    conn.close()


def deal_frequency(ele):
    try:
        if np.isnan(ele):
            pass
    except:
        ele = ele.split("%")[0]
    
    return ele


if __name__ == '__main__':
    main()