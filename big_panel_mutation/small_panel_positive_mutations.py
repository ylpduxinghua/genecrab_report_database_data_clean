import sys
sys.path.append('/duxinghua/genecrab_report_database_data_clean')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info
from datetime import datetime
import pandas as pd
import numpy as np
import re


def main():
    # 获取数据库snv信息 140240
    conn = get_database_conn()
    sql = '''SELECT
                report_id,
                report_sample_id,
                report_mutation_id,
                frequency,
                pccn,
                log2_ratio 
            FROM
                report_mutation_cnv_desc 
            WHERE
                report_mutation_id IN (
                SELECT DISTINCT
                    id 
                FROM
                    report_mutations m 
                WHERE
                    data_type = 'CNV' 
                    AND selected = 1 UNION
                SELECT DISTINCT
                    id 
                FROM
                    small_panel_patient_positive_results 
                WHERE
                    data_type = 'CNV' 
                    AND selected = 1 
                    AND frequency <> '-' 
                );'''
    result = query_database(conn, sql)

    conn.close()


    # 创建存储数据的dataframe
    columns_list = ['report_mutation_id', 'report_id', 'gene', 'data_type', 'genomic_change',
                    'transcript', 'dna_region', 'mutation_c', 'mutation_p', 'cnv_change_type',
                    'chromosome', 'position', 'ref', 'alt',  'fusion_variant',
                    'chromosome1', 'chromosome2', 'gene1', 'gene2', 'exon1',
                    'exon2', 'transcript1', 'transcript2', 'fusion_source', 'fusion_type',
                    'pair_order']

    df = pd.DataFrame(data=None, columns=columns_list)
    # 存储数据
    for tuple in result:
        report_mutation_id, report_id, gene, data_type, genomic_change,\
        transcript, dna_region, mutation_c, mutation_p, cnv_change_type,\
        chromosome, position, ref, alt,  chromosome1,\
        chromosome2, gene1, gene2, exon1, exon2,\
        transcript1, transcript2, fusion_source, pair_order = tuple
        fusion_variant = np.nan

        if gene1 == None or gene2 == None:
            pass
        else:
            fusion_variant = gene1 + '-' + gene2 + ' 融合'

        df.loc[report_mutation_id, :] = [report_mutation_id, report_id, gene, data_type, genomic_change,
                        transcript, dna_region, mutation_c, mutation_p, cnv_change_type,
                        chromosome, position, ref, alt, fusion_variant,
                        chromosome1, chromosome2, gene1, gene2, exon1,
                        exon2, transcript1, transcript2, fusion_source, pair_order,
                        pair_order]
    df.to_csv('small_panel_mutations.tsv', sep='\t')

    # “基因组改变“信息
    df.loc[df.loc[:, 'data_type'].isin(['CNV', 'FUSION']), 'genomic_change'] = np.nan
    df.loc[:, 'genomic_change'] = df.loc[:, 'genomic_change'].apply(deal_genomic_change)

    # DNA区域信息
    df.loc[df.loc[:, 'dna_region'] == '', 'dna_region'] = np.nan

    # 核苷酸改变
    df.loc[:, 'mutation_c'] = df.loc[:, 'mutation_c'].apply(deal_mutation_c)

    # 转录本号
    df.loc[df.loc[:, 'transcript'] == '', 'transcript'] = np.nan

    # 氨基酸改变
    df.loc[:, 'mutation_p'] = df.loc[:, 'mutation_p'].apply(deal_mutation_p)

    # 融合染色体1， 融合染色体2， exon1, exon2
    df.loc[df.loc[:, 'chromosome1'] == '', 'chromosome1'] = np.nan
    df.loc[df.loc[:, 'chromosome2'] == '', 'chromosome2'] = np.nan
    df.loc[df.loc[:, 'exon1'] == 0, 'exon1'] = np.nan
    df.loc[df.loc[:, 'exon2'] == 0, 'exon2'] = np.nan

    # 融合来源
    df.loc[:, 'fusion_source'] = df.loc[:, 'fusion_source'].map({0:'DNA融合', 1:'RNA融合'})

    # 融合类型
    df.loc[:, 'fusion_type'] = df.loc[:, 'fusion_type'].apply(deal_fusion_type)

    # 融合方向
    df.loc[df.loc[:, 'pair_order'] == '', 'pair_order'] = np.nan
    df.loc[df.loc[:, 'pair_order'] == 'NA-5', 'pair_order'] = 'NA->5'
    df.loc[df.loc[:, 'pair_order'] == '5->NA', 'pair_order'] = 'NA->5'

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
    big_panel_mutations_sql = '''create table small_panel_mutations_clean_final(
        id int(10) not null auto_increment,
        report_mutation_id int(10) unsigned not null comment "report_mutations表id",
        reports_table_id int(10) unsigned not null comment 'reports表id',
        gene varchar(255) default null comment "基因",
        data_type varchar(20) default null comment "变异类型， SNV/CNV/FUSION",
        genomic_change varchar(255) default null comment "基因组改变",
        transcript varchar(30) default null comment "转录本号",
        dna_region varchar(30) default null comment "DNA区域",
        mutation_c varchar(255) default null comment "核苷酸变化",
        mutation_p varchar(255) default null comment "氨基酸变化",
        cnv_change_type varchar(30) default null comment "拷贝数变异类型",
        chromosome varchar(255) default null comment "染色体编号",
        position int(11) default null comment "突变起始位置",
        ref varchar(255) default null comment "参考序列",
        alt varchar(255) default null comment "变异序列",
        fusion_variant varchar(255) default null comment "融合变异",
        chromosome1 varchar(255) default null comment "融合染色体1",
        chromosome2 varchar(255) default null comment "融合染色体2",
        gene1 varchar(255) default null comment "融合基因1",
        gene2 varchar(255) default null comment "融合基因2",
        exon1 int(11) default null comment "融合外显子1",
        exon2 int(11) default null comment "融合外显子2",
        transcript1 varchar(255) default null comment "融合转录本1",
        transcript2 varchar(255) default null comment "融合转录本2",
        fusion_source varchar(30) default null comment "融合来源， DNA/RNA",
        fusion_type varchar(30) default null comment "融合类型",
        pair_order varchar(30) default null comment "融合方向",
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        primary key (id)
    )'''
    create_table(conn, 'small_panel_mutations_clean_final', big_panel_mutations_sql)
    conn.close()

    # 插入数据
    conn = get_database_conn()
    cur = conn.cursor()
    insert_sql = f'''insert into small_panel_mutations_clean_final (
                    report_mutation_id, reports_table_id, gene, data_type, genomic_change,
                        transcript, dna_region, mutation_c, mutation_p, cnv_change_type,
                        chromosome, position, ref, alt, fusion_variant,
                        chromosome1, chromosome2, gene1, gene2, exon1,
                        exon2, transcript1, transcript2, fusion_source, fusion_type,
                        pair_order, created_at, lot_number, lot_date_range) values (%s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s)'''
    insert_table_many_info(conn, cur, insert_sql, result_new)
    cur.close()
    conn.close()





def deal_genomic_change(ele):
    try:
        if ele == None or np.isnan(ele):
            return ele
    except:
        if ele in ['', '-']:
            return np.nan
        else:
            ele = ele.split(':')[1]
    # del
    regex1 = r'(g\.(\d+)del)[A-Z]+'
    regex1_2 = r'(g\.(\d+)_(\d+)del)(\d+)$'
    regex1_3 = r'(g\.(\d+)_(\d+)del)[A-Z]$'
    # dup
    regex2 = r'(g\.(\d+)dup)[A-Z]+'
    regex2_2 = r'(g\.(\d+)_(\d+)dup)[A-Z]+'

    match1 = re.search(regex1, ele)
    match1_2 = re.search(regex1_2, ele)
    match1_3 = re.search(regex1_3, ele)
    match2 = re.search(regex2, ele)
    match2_2 = re.search(regex2_2, ele)

    if match1:
        ele = match1.group(1)
    elif match1_2:
        ele = match1_2.group(1)
    elif match1_3:
        ele = match1_3.group(1)
    elif match2:
        ele = match2.group(1)
    elif match2_2:
        ele = match2_2.group(1)
    else:
        pass
    return ele


def deal_mutation_c(ele):
    # dup
    regex1 = r'(c\.(\d+)_(\d+)dup)[A-Z]+$'
    regex1_2 = r'(c\.(\d+)dup)[A-Z]+$'
    regex1_3 = r'(c\.(\d+)[-+](\d+)_(\d+)dup)[A-Z]+$'
    regex1_4 = r'(c\.(\d+)_(\d+)[-+](\d+)dup)[A-Z]+$'
    regex1_5 = r'(c\.(\d+)[-+](\d+)_(\d+)[-+](\d+)dup)[A-Z]+$'

    # del
    regex2 = r'(c\.(\d+)_(\d+)del)[A-Z]+$'
    regex2_2 = r'(c\.(\d+)del)[A-Z]+$'
    regex2_3 = r'(c\.(\d+)[-+](\d+)_(\d+)del)[A-Z]+$'
    regex2_4 = r'(c\.(\d+)_(\d+)[-+](\d+)del)[A-Z]+$'
    regex2_5 = r'(c\.(\d+)[-+](\d+)_(\d+)[-+](\d+)del)[A-Z]+$'
    regex2_6 = r'(c\.(\d+)_(\d+)del)(\d+)$'
    
    try:
        if ele == None or np.isnan(ele):
            pass
    except:
        match1 = re.search(regex1, ele)
        match1_2 = re.search(regex1_2, ele)
        match1_3 = re.search(regex1_3, ele)
        match1_4 = re.search(regex1_4, ele)
        match1_5 = re.search(regex1_5, ele)
        match2 = re.search(regex2, ele)
        match2_2 = re.search(regex2_2, ele)
        match2_3 = re.search(regex2_3, ele)
        match2_4 = re.search(regex2_4, ele)
        match2_5 = re.search(regex2_5, ele)
        match2_6 = re.search(regex2_6, ele)
        if ele == '' or ele == '.':
            ele = np.nan
        elif match1:
            ele = match1.group(1)
        elif match1_2:
            ele = match1_2.group(1)
        elif match1_3:
            ele = match1_3.group(1)
        elif match1_4:
            ele = match1_4.group(1)
        elif match1_5:
            ele = match1_5.group(1)
        elif match2:
            ele = match2.group(1)
        elif match2_2:
            ele = match2_2.group(1)
        elif match2_3:
            ele = match2_3.group(1)
        elif match2_4:
            ele = match2_4.group(1)
        elif match2_5:
            ele = match2_5.group(1)
        elif match2_6:
            ele = match2_6.group(1)
        else:
            pass
    return ele


def deal_mutation_p(ele):
    # del
    regex1 = r'(p\.[A-Z](\d+)_[A-Z](\d+)del)[A-Z]+$'
    regex1_2 = r'(p\.[A-Z](\d+)_[A-Z](\d+)del)(\d+)$'
    regex1_3 = r'(p\.[A-Z](\d+)del)[A-Z]+$'
    # dup
    regex2 = r'(p\.[A-Z](\d+)_[A-Z](\d+)dup)[A-Z]+$'
    regex2_2 = r'(p\.[A-Z](\d+)dup)[A-Z]+$'

    # fs
    regex3 = r'p\.[A-Z](\d+)\*fs\*(\d+)$'
    
    try:
        if np.isnan(ele):
            pass
    except:
        match1 = re.search(regex1, ele)
        match1_2 = re.search(regex1_2, ele)
        match1_3 = re.search(regex1_3, ele)

        match2 = re.search(regex2, ele)
        match2_2 = re.search(regex2_2, ele)

        match3 = re.search(regex3, ele)
        if ele in ['', '.']:
            ele = np.nan
        elif match1:
            ele = match1.group(1)
        elif match1_2:
            ele = match1_2.group(1)
        elif match1_3:
            ele = match1_3.group(1)
        elif match2:
            ele = match2.group(1)
        elif match2_2:
            ele = match2_2.group(1)
        elif match3:
            ele = match3.group(1)
        else:
            pass
    return ele


def deal_fusion_type(ele):
    mapping_dict = {
        '5->3': '融合',
        '3->3': '重排',
        '5->5': '重排',
        'NA-5': '间区融合',
        'NA->3': '间区融合',
        '5->NA': '间区融合',
        "": None
    }

    try:
        if ele == None or np.isnan(ele):
            return ele
    except:
        return mapping_dict[ele]


if __name__ == '__main__':
    main()