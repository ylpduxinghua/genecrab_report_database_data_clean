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
                info.id,
                info.report_id,
                info.pd_l1_report_sample_id,
                info.pd_l1_tumor_cell_positive_ratio,-- 阳性肿瘤细胞占比
                info.pd_l1_immune_cell_positive_ratio,-- 阳性免疫细胞占比
                info.pd_l1_cps,-- cps检测结果  -对应pdf报告的pdl1蛋白表达检测结果cps值
                info.tps_positive,-- tps检测结果 -对应pdf报告的pdl1蛋白表达检测结果tps值
                info.pd_1_immune_cell_positive_ratio,-- 免疫细胞
                info.mlh1_positive,-- MLH1结果
                info.msh2_positive,-- MSH2结果
                info.msh6_positive,-- MSH6结果
                info.pms2_positive,-- PMS2结果
                info.mmr_result -- 判读结果
                
            FROM
                report_pd_l1_info info 
            WHERE
                pd_l1_report_sample_id IN ( SELECT pd_l1_report_samples_table_id FROM pd_l1_sample_information_clean_final );'''

    result = query_database(conn, sql)

    conn.close()

    # 创建存储数据的dataframe
    columns_list = ['report_id', 'pd_l1_report_sample_id', 'pd_l1_tumor_cell_positive_ratio',
        'pd_l1_immune_cell_positive_ratio', 'pd_l1_cps', 'tps_positive', 'pd_1_immune_cell_positive_ratio',
        'mlh1_positive', 'msh2_positive', 'msh6_positive', 'pms2_positive', 'mmr_result']

    df = pd.DataFrame(data=None, columns=columns_list)

    # 存储数据
    for tuple in result:
        id, report_id, pd_l1_report_sample_id, pd_l1_tumor_cell_positive_ratio,\
            pd_l1_immune_cell_positive_ratio, pd_l1_cps,  tps_positive, pd_1_immune_cell_positive_ratio,\
            mlh1_positive, msh2_positive, msh6_positive, pms2_positive, mmr_result = tuple
        df.loc[id, 'report_id'] = report_id
        df.loc[id, 'pd_l1_report_sample_id'] = pd_l1_report_sample_id
        df.loc[id, 'pd_l1_tumor_cell_positive_ratio'] = pd_l1_tumor_cell_positive_ratio
        df.loc[id, 'pd_l1_immune_cell_positive_ratio'] = pd_l1_immune_cell_positive_ratio
        df.loc[id, 'pd_l1_cps'] = pd_l1_cps
        df.loc[id, 'tps_positive'] = tps_positive
        df.loc[id, 'pd_1_immune_cell_positive_ratio'] = pd_1_immune_cell_positive_ratio
        df.loc[id, 'mlh1_positive'] = mlh1_positive
        df.loc[id, 'msh2_positive'] = msh2_positive
        df.loc[id, 'msh6_positive'] = msh6_positive
        df.loc[id, 'pms2_positive'] = pms2_positive
        df.loc[id, 'mmr_result'] = mmr_result

    df.to_csv('pd_l1_result.tsv', sep='\t')
    # df = pd.read_csv(sys.argv[1], sep='\t', index_col=0)
    
    # ''值转为nan
    df.loc[df.loc[:, 'pd_l1_tumor_cell_positive_ratio'] == '', 'pd_l1_tumor_cell_positive_ratio'] = np.nan
    df.loc[df.loc[:, 'pd_l1_immune_cell_positive_ratio'] == '', 'pd_l1_immune_cell_positive_ratio'] = np.nan
    df.loc[df.loc[:, 'pd_l1_cps'] == '', 'pd_l1_cps'] = np.nan
    df.loc[df.loc[:, 'tps_positive'] == '', 'tps_positive'] = np.nan
    df.loc[df.loc[:, 'tps_positive'] == '合格', 'tps_positive'] = np.nan
    df.loc[df.loc[:, 'pd_1_immune_cell_positive_ratio'] == '', 'pd_1_immune_cell_positive_ratio'] = np.nan
    df.loc[df.loc[:, 'mlh1_positive'] == '', 'mlh1_positive'] = np.nan
    df.loc[df.loc[:, 'msh2_positive'] == '', 'msh2_positive'] = np.nan
    df.loc[df.loc[:, 'msh6_positive'] == '', 'msh6_positive'] = np.nan
    df.loc[df.loc[:, 'pms2_positive'] == '', 'pms2_positive'] = np.nan
    df.loc[df.loc[:, 'mmr_result'] == '/', 'mmr_result'] = np.nan

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
    pd_l1_result_sql = '''create table pd_l1_result_clean_final (
        id int(10) not null auto_increment,
        report_id int(10) not null comment "reports表id",
        report_sample_id int(10) not null comment "pd_l1_report_sample_id表id",
        pd_l1_tumor_cell_positive_ratio varchar(255) default null comment "pdl1阳性肿瘤细胞占比(sp142)",
        pd_l1_immune_cell_positive_ratio varchar(255) default null comment "pdl1阳性免疫细胞占比(sp142)",
        pd_l1_cps varchar(255) default null comment "pdl1蛋白表达检测结果cps值(22c3)",
        tps_positive varchar(255) default null comment "pdl1蛋白表达检测结果tps值(22c3)",
        pd_1_immune_cell_positive_ratio varchar(255) default null comment "pd1阳性免疫细胞占比",
        mlh1_positive varchar(255) default null comment "mlh1检测结果",
        msh2_positive varchar(255) default null comment "msh2检测结果",
        msh6_positive varchar(255) default null comment "msh6检测结果",
        pms2_positive varchar(255) default null comment "pms2检测结果",
        mmr_result varchar(255) default null comment "mmr判断结果",
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        primary key (id)
    )'''


    create_table(conn, 'pd_l1_result_clean_final', pd_l1_result_sql)
    conn.close()

    # 插入数据
    conn = get_database_conn()
    cur = conn.cursor()
    insert_sql = f'''insert into pd_l1_result_clean_final (
                    report_id, report_sample_id, pd_l1_tumor_cell_positive_ratio,
                    pd_l1_immune_cell_positive_ratio, pd_l1_cps, tps_positive,
                    pd_1_immune_cell_positive_ratio, mlh1_positive, msh2_positive,
                    msh6_positive, pms2_positive, mmr_result, created_at, lot_number,
                    lot_date_range) values (%s, %s, %s,
                                            %s, %s, %s,
                                            %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s)'''
    insert_table_many_info(conn, cur, insert_sql, result_new)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()