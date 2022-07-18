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
    sql = '''SELECT DISTINCT
                r.id,
                r.report_id,
                r.gene,
            CASE
                    ifnull( x.mutation_level, r.mutation_level_id ) 
                    WHEN '0' THEN
                    3 
                    WHEN 1 THEN
                    1 
                    WHEN 2 THEN
                    2 ELSE NULL 
                END AS mutation_level_id,
                r.data_type,
                r.mutation_g_reviewed,
                r.exon,
                r.mutation_c,
                r.mutation_p,
                r.transcript,
                r.type_specific,
                r.sift_score,
                r.sift,
                r.polyphen2,
                r.is_germline,
                r.zygosity,
                r.clinsig,
                r.ExAC_ALL,
                r.ExAC_EAS,
                r.gnomAD_genome_ALL,
                r.gnomAD_genome_EAS,
                r.1000g2015aug_all,
                r.1000g2015aug_eas,
                r.cnv_result,
                r.cytoband,
                r.display_mutation,
                r.fusion_gene1,
                r.fusion_gene2,
                r.exon1,
                r.exon2,
                d.chromosome,
                d.coordinate,
                d.ref,
                d.alt,
                f.chromosome1,
                f.chromosome2,
                f.transcript1,
                f.transcript2,
                f.fusion_type,
                f.pair_order 
            FROM
                report_mutations r
                LEFT JOIN report_mutation_desc d ON r.id = d.report_mutation_id
                LEFT JOIN report_mutation_fusion_desc f ON r.id = f.report_mutation_id
                LEFT JOIN (
                SELECT
                    report_id,
                    mutation_id,
                IF
                    (
                        SUBSTR( GROUP_CONCAT( DISTINCT mutation_level_id ), 1, 1 ) < SUBSTR( GROUP_CONCAT( DISTINCT mutation_level_id ), -1, 1 ),
                        SUBSTR( GROUP_CONCAT( DISTINCT mutation_level_id ), 1, 1 ),
                    SUBSTR( GROUP_CONCAT( DISTINCT mutation_level_id ), -1, 1 )) AS mutation_level 
                FROM
                    report_mutation_targeted_drugs 
                WHERE
                    selected = 1 
                    AND is_valid = 1 
                GROUP BY
                    report_id,
                    mutation_id 
                ) x ON r.id = x.mutation_id 
                AND r.report_id = x.report_id 
            WHERE
                r.selected = 1;'''
    result = query_database(conn, sql)

    conn.close()


    # 创建存储数据的dataframe
    columns_list = ['report_mutation_id',
                    'report_id', 'gene', 'mutation_level', 'data_type',
                    'genomic_change', 'transcript', 'dna_region', 'mutation_p', 'mutation_c',
                    'consequence_type', 'sift_predict', 'sift_score', 'polyphen2_predict', 'genomic_source',
                    'zygosity', 'acmg', 'chromosome', 'position', 'ref',
                    'alt', 'exac_all', 'exac_eas', 'gnomad_genome_all', 'gnomad_genome_eas',
                    '1000g2015aug_all', '1000g2015aug_eas', 'cnv_change_type', 'cytoband', 'fusion_variant',
                    'gene1', 'gene2', 'exon1', 'exon2', 'chromosome1',
                    'chromosome2', 'transcript1', 'transcript2', 'fusion_source', 'fusion_type',
                    'pair_order']

    df = pd.DataFrame(data=None, columns=columns_list)
    # 存储数据
    for tuple in result:
        id,\
        report_id, gene, mutation_level_id, data_type,\
        mutation_g_reviewed, exon, mutation_c, mutation_p, transcript,\
        type_specific, sift_score, sift, polyphen2, is_germline,\
        zygosity, clinsig, ExAC_ALL, ExAC_EAS, gnomAD_genome_ALL,\
        gnomAD_genome_EAS, g2015aug_all, g2015aug_eas, cnv_result, cytoband,\
        display_mutation, fusion_gene1, fusion_gene2, exon1, exon2,\
        chromosome, coordinate, ref, alt, chromosome1,\
        chromosome2, transcript1, transcript2, fusion_source, pair_order = tuple

        fusion_type = np.nan

        df.loc[id, :] = [id,\
                        report_id, gene, mutation_level_id, data_type,\
                        mutation_g_reviewed, transcript, exon, mutation_p, mutation_c, \
                        type_specific, sift, sift_score, polyphen2, is_germline,\
                        zygosity, clinsig, chromosome, coordinate, ref, \
                        alt, ExAC_ALL, ExAC_EAS, gnomAD_genome_ALL, gnomAD_genome_EAS,\
                        g2015aug_all, g2015aug_eas, cnv_result, cytoband, display_mutation,\
                        fusion_gene1, fusion_gene2, exon1, exon2, chromosome1,\
                        chromosome2, transcript1, transcript2, fusion_source, fusion_type,\
                        pair_order]

    df.to_csv('big_panel_mutations.tsv', sep='\t')
    # df = pd.read_csv(sys.argv[1], sep='\t', index_col=0)
    ####### 数据归一 ########

    # 变异级别
    df.loc[:, 'mutation_level'] = df.loc[:, 'mutation_level'].map({0:3, 1:1, 2:2})

    # “基因组改变“信息
    df.loc[:, 'genomic_change'] = df.loc[:, 'genomic_change'].apply(deal_genomic_change)

    # DNA区域信息
    df.loc[df.loc[:, 'dna_region'] == '', 'dna_region'] = np.nan

    # 核苷酸改变
    df.loc[:, 'mutation_c'] = df.loc[:, 'mutation_c'].apply(deal_mutation_c)

    # 转录本号
    df.loc[df.loc[:, 'transcript'] == '', 'transcript'] = np.nan

    # 氨基酸改变
    df.loc[:, 'mutation_p'] = df.loc[:, 'mutation_p'].apply(deal_mutation_p)

    # 功能改变类型
    df.loc[df.loc[:, 'data_type'].isin(['CNV', 'FUSION']), 'consequence_type'] = np.nan
    df.loc[:, 'consequence_type'] = df.loc[:, 'consequence_type'].apply(deal_consequence_type)

    # sift功能预测分值
    df.loc[df.loc[:, 'sift_score'] == '', 'sift_score'] = np.nan
    df.loc[df.loc[:, 'sift_score'] == '.', 'sift_score'] = '0'
    df.loc[df.loc[:, 'sift_score'] == 'D', 'sift_score'] = 'Deleterious'
    df.loc[df.loc[:, 'sift_score'] == 'T', 'sift_score'] = 'Tolerated'
    # 保留两位小数
    df.loc[:, 'sift_score'] = df.loc[:, 'sift_score'].apply(keep_two_digit)

    # sift功能预测结果
    df.loc[df.loc[:, 'sift_predict'].isin(['.', '-']), 'sift_predict'] = np.nan

    # polyphen2预测结果
    df.loc[df.loc[:, 'polyphen2_predict'].isin(['.', '-']), 'polyphen2_predict'] = np.nan
    df.loc[df.loc[:, 'polyphen2_predict'] == 'D', 'polyphen2_predict'] = 'Probably damaging'
    df.loc[df.loc[:, 'polyphen2_predict'] == 'B', 'polyphen2_predict'] = 'Benign'
    df.loc[df.loc[:, 'polyphen2_predict'] == 'P', 'polyphen2_predict'] = 'Possibly damaging'

    # 是否胚系
    df.loc[:, 'genomic_source'] = df.loc[:, 'genomic_source'].map({0:'体细胞', 1:'胚系'})

    # 等位基因状态
    df.loc[:, 'zygosity'] = df.loc[:, 'zygosity'].map({0:'杂合', 1:'纯合'})

    # acmg
    df.loc[:, 'acmg'] =  df.loc[:, 'acmg'].apply(deal_acmg)
    df.loc[df.loc[:, 'genomic_source'] == '体细胞', 'acmg'] = np.nan

    # chromosome
    df.loc[df.loc[:, 'chromosome'] == '7', 'chromosome'] = 'chr7'

    # ExAC_all
    df.loc[df.loc[:, 'exac_all'].isin(['.', '']), 'exac_all'] = np.nan
    df.loc[:, 'exac_all'] = df.loc[:, 'exac_all'].apply(as_num)

    # ExAC_EAS, gnomAD, 1000g
    df.loc[df.loc[:, 'exac_eas'] == '', 'exac_eas'] = np.nan
    df.loc[df.loc[:, 'gnomad_genome_all'] == '', 'gnomad_genome_all'] = np.nan
    df.loc[df.loc[:, 'gnomad_genome_eas'] == '', 'gnomad_genome_eas'] = np.nan
    df.loc[df.loc[:, '1000g2015aug_all'] == '', '1000g2015aug_all'] = np.nan
    df.loc[df.loc[:, '1000g2015aug_eas'] == '', '1000g2015aug_eas'] = np.nan

    # cnv change type; cytoband
    df.loc[df.loc[:, 'cnv_change_type'] == '', 'cnv_change_type'] = np.nan
    df.loc[df.loc[:, 'cnv_change_type'] == 'GAIN', 'cnv_change_type'] = 'Gain'
    df.loc[df.loc[:, 'cnv_change_type'] == 'LOSS', 'cnv_change_type'] = 'Loss'
    df.loc[df.loc[:, 'cytoband'] == '', 'cytoband'] = np.nan

    # 融合变异
    df.loc[~(df.loc[:, 'data_type'] == 'FUSION'), 'fusion_variant'] = np.nan

    # 融合基因1， 融合基因2, 染色体1， 染色体2, exon1, exon2
    df.loc[df.loc[:, 'gene1'] == '', 'gene1'] = np.nan
    df.loc[df.loc[:, 'gene2'] == '', 'gene2'] = np.nan
    df.loc[df.loc[:, 'chromosome1'] == '', 'chromosome1'] = np.nan
    df.loc[df.loc[:, 'chromosome2'] == '', 'chromosome2'] = np.nan
    df.loc[df.loc[:, 'exon1'] == '', 'exon1'] = np.nan
    df.loc[df.loc[:, 'exon2'] == '', 'exon2'] = np.nan
    df.loc[df.loc[:, 'exon1'] == 0, 'exon1'] = np.nan
    df.loc[df.loc[:, 'exon2'] == 0, 'exon2'] = np.nan

    # 融合来源
    df.loc[:, 'fusion_source'] = df.loc[:, 'fusion_source'].map({0:'DNA融合', 1:'RNA融合'})

    # 融合类型
    # df.loc[df.loc[:, 'data_type'].isin(['CNV', 'SNV']), 'fusion_type'] = np.nan
    df.loc[:, 'fusion_type'] = df.loc[:, 'pair_order'].apply(deal_fusion_type)

    # 融合方向
    df.loc[df.loc[:, 'pair_order'] == '', 'pair_order'] = np.nan
    df.loc[df.loc[:, 'pair_order'] == 'NA-5', 'pair_order'] = 'NA->5'
    # df.loc[df.loc[:, 'pair_order'] == '5->NA', 'pair_order'] = 'NA->5'

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

    # 创建大panel突变表
    conn = get_database_conn()
    big_panel_mutations_sql = '''create table big_panel_mutations_clean_final(
        id int(10) not null auto_increment,
        report_mutation_id int(10) unsigned not null comment "report_mutations表id",
        reports_table_id int(10) unsigned not null comment 'reports表id',
        gene varchar(255) default null comment "基因",
        mutation_level tinyint default null comment "tier 分级",
        data_type varchar(20) default null comment "变异类型， SNV/CNV/FUSION",
        genomic_change varchar(255) default null comment "基因组改变",
        transcript varchar(30) default null comment "转录本号",
        dna_region varchar(30) default null comment "DNA区域",
        mutation_p varchar(255) default null comment "氨基酸变化",
        mutation_c varchar(255) default null comment "核苷酸变化",
        consequence_type varchar(30) default null comment "功能改变类型",
        sift_predict varchar(20) default null comment "Sift功能预测结果",
        sift_score varchar(20) default null comment "Sift功能预测分值",
        polyphen2_predict varchar(20) default null comment "Polyphen2功能预测结果",
        genomic_source varchar(20) default null comment "胚系/体系来源",
        zygosity varchar(20) default null comment "纯合/杂合",
        acmg varchar(30) default null comment "acmg分级",
        chromosome varchar(255) default null comment "染色体编号",
        position int(11) default null comment "突变起始位置",
        ref varchar(255) default null comment "参考序列",
        alt varchar(255) default null comment "变异序列",
        exac_all decimal(15,10) default null comment "ExAC数据库所有人群的等位基因频率",
        exac_eas decimal(15,10) default null comment "ExAC数据库东亚人群的等位基因频率",
        gnomad_genome_all decimal(15,10) default null comment "gnomAD数据库所有人群的等位基因频率",
        gnomad_genome_eas decimal(15,10) default null comment "gnomAD数据库东亚人群的等位基因频率",
        1000g2015aug_all decimal(15,10) default null comment "1000g数据库所有人群的等位基因频率",
        1000g2015aug_eas decimal(15,10) default null comment "1000g数据库东亚人群的等位基因频率",
        cnv_change_type varchar(30) default null comment "拷贝数变异类型",
        cytoband varchar(30) default null comment "染色体区段",
        fusion_variant varchar(255) default null comment "融合变异",
        gene1 varchar(255) default null comment "融合基因1",
        gene2 varchar(255) default null comment "融合基因2",
        exon1 int(11) default null comment "融合外显子1",
        exon2 int(11) default null comment "融合外显子2",
        chromosome1 varchar(255) default null comment "融合染色体1",
        chromosome2 varchar(255) default null comment "融合染色体2",
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
    create_table(conn, 'big_panel_mutations_clean_final', big_panel_mutations_sql)
    conn.close()

    # 插入数据
    conn = get_database_conn()
    cur = conn.cursor()
    insert_sql = f'''insert into big_panel_mutations_clean_final (
                    report_mutation_id, reports_table_id, gene, mutation_level, data_type,
                    genomic_change, transcript, dna_region, mutation_p, mutation_c,
                    consequence_type, sift_predict, sift_score, polyphen2_predict, genomic_source,
                    zygosity, acmg, chromosome, position, ref,
                    alt, exac_all, exac_eas, gnomad_genome_all, gnomad_genome_eas,
                    1000g2015aug_all, 1000g2015aug_eas, cnv_change_type, cytoband, fusion_variant,
                    gene1, gene2, exon1, exon2, chromosome1,
                    chromosome2, transcript1, transcript2, fusion_source, fusion_type,
                    pair_order, created_at, lot_number, lot_date_range) values (%s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s)'''
    insert_table_many_info(conn, cur, insert_sql, result_new)
    cur.close()
    conn.close()


def deal_acmg(ele):
    mapping_dict = {
        '-': np.nan,
        'g:可能致病':'可能致病',
        'Pathogenic':'致病',
        '可能致病': '可能致病',
        '可能良性': '可能良性',
        '意义未明': '意义未明',
        '致病': '致病',
        '致病/可能致病': np.nan,
        '致病性存在争议': np.nan
    }

    try:
        if ele == None or np.isnan(ele):
            pass
    except:
        if ele in mapping_dict:
            ele = mapping_dict[ele]
        else:
            ele = np.nan
        
    return ele


def keep_two_digit(ele):
    try:
        if ele == None or np.isnan(ele):
            pass
    except:
        ele = "%.2f" % (float(ele))
    return ele


def as_num(ele):
    '科学计数法转化为小数'
    try:
        if ele == None or np.isnan(ele):
            return ele
    except:
        ele = float(ele)
    return ele


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
    regex2_5 = r'(c\.(\d+)[-+]*(\d+)_(\d+)[-+](\d+)del)[A-Z]+$'
    regex2_6 = r'(c\.(\d+)_(\d+)del)(\d+)$'
    regex2_7 = r'(c\.(\d+)[-+](\d+)_(\d+)[-+](\d+)del)(\d)+$'
    regex2_8 = r'(c\.(\d+)_(\d+)[-+](\d+)del)(\d)+$'
    regex2_9 = r'(c\.(\d+)[-+](\d+)_(\d+)del)(\d)+$'
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
        match2_7 = re.search(regex2_7, ele)
        match2_8 = re.search(regex2_8, ele)
        match2_9 = re.search(regex2_9, ele)
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
        elif match2_7:
            ele = match2_7.group(1)
        elif match2_8:
            ele = match2_8.group(1)
        elif match2_9:
            ele = match2_9.group(1)
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
    regex3 = r'(p\.[A-Z](\d+)\*)fs\*(\d+)$'
    
    try:
        if ele == None or np.isnan(ele):
            pass
    except:
        match1 = re.search(regex1, ele)
        match1_2 = re.search(regex1_2, ele)
        match1_3 = re.search(regex1_3, ele)

        match2 = re.search(regex2, ele)
        match2_2 = re.search(regex2_2, ele)

        match3 = re.search(regex3, ele)
        if ele in ['', '.'] or ele == 'c.3027A>G':
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
        'NA->5': '间区融合',
        "": None
    }

    try:
        if ele == None or np.isnan(ele):
            return ele
    except:
        return mapping_dict[ele]


def deal_consequence_type(ele):
    mapping_dict = {'错义突变':'missense_variant',
                    'missense mutation':'missense_variant',
                    '框内插入突变':'inframe_insertion',
                    '框内缺失突变': 'inframe_deletion',
                    '移码突变':'frameshift_variant',
                    '无义突变':'stop_gained',
                    'nonsense mutation':'stop_gained',
                    '同义突变':'synonymous_variant',
                    '终止密码子碱基替换突变': None,
                    '剪接受体单核苷酸突变':'splice_acceptor_variant',
                    '剪接受体缺失突变':'splice_acceptor_variant',
                    '剪接受体插入突变':'splice_acceptor_variant',
                    '剪接受体碱基替换突变':'splice_acceptor_variant',
                    'Splice acceptor base substitution':'splice_acceptor_variant',
                    '剪接供体单核苷酸突变':'splice_donor_variant',
                    'Splice donor single nucleotide mutation':'splice_donor_variant',
                    '剪接供体缺失突变':'splice_donor_variant',
                    '剪接供体插入突变':'splice_donor_variant',
                    '剪接供体碱基替换突变':'splice_donor_variant',
                    '内含子区域单核苷酸突变':'intron_variant',
                    '内含子碱基替换突变':'intron_variant',
                    '内含子缺失突变':'intron_variant',
                    '基因间区单核苷酸突变':'intergenic_variant',
                    '缺失':None,
                    '未分类':None,
                    '': None}
    try:
        if ele == None or np.isnan(ele):
            return ele
    except:
        return mapping_dict[ele]


if __name__ == '__main__':
    main()