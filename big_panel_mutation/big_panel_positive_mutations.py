from re import S
import sys
sys.path.append('/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗脚本')
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
                        r.mutation_level_id,
                        r.data_type,
                        r.exon_state,
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
                        left JOIN report_mutation_desc d ON r.id = d.report_mutation_id 
                        left join report_mutation_fusion_desc f on r.id = f.report_mutation_id
                    WHERE
                        r.selected = 1;'''
    result = query_database(conn, sql)

    conn.close()


    # 创建存储数据的dataframe
    columns_list = ['report_mutation_id',
                    'report_id', 'gene', 'mutation_level', 'data_type', 'snv_change_type',
                    'genomic_change', 'transcript', 'dna_region', 'mutation_p', 'mutation_c',
                    'consequence_type', 'sift_predict', 'sift_score', 'polyphen2_predict', 'genomic_source',
                    'zygosity', 'acmg', 'chromosome', 'position', 'ref',
                    'alt', 'exac_all', 'exac_eas', 'genomad_genome_all', 'genomad_genome_eas',
                    '1000g2015aug_all', '1000g2015aug_eas', 'cnv_change_type', 'cytoband', 'fusion_variant',
                    'gene1', 'gene2', 'exon1', 'exon2', 'chromosome1',
                    'chromosome2', 'transcript1', 'transcript2', 'fusion_source', 'fusion_type',
                    'pair_order']

    df = pd.DataFrame(data=None, columns=columns_list)
    # 存储数据
    for tuple in result:
        id,\
        report_id, gene, mutation_level_id, data_type, exon_state,\
        mutation_g_reviewed, exon, mutation_c, mutation_p, transcript,\
        type_specific, sift_score, sift, polyphen2, is_germline,\
        zygosity, clinsig, ExAC_ALL, ExAC_EAS, gnomAD_genome_ALL,\
        gnomAD_genome_EAS, g2015aug_all, g2015aug_eas, cnv_result, cytoband,\
        display_mutation, fusion_gene1, fusion_gene2, exon1, exon2,\
        chromosome, coordinate, ref, alt, chromosome1,\
        chromosome2, transcript1, transcript2, fusion_source, pair_order = tuple

        fusion_type = type_specific

        df.loc[id, :] = [id,\
                        report_id, gene, mutation_level_id, data_type, exon_state,\
                        mutation_g_reviewed, transcript, exon, mutation_p, mutation_c, \
                        type_specific, sift, sift_score, polyphen2, is_germline,\
                        zygosity, clinsig, chromosome, coordinate, ref, \
                        alt, ExAC_ALL, ExAC_EAS, gnomAD_genome_ALL, gnomAD_genome_EAS,\
                        g2015aug_all, g2015aug_eas, cnv_result, cytoband, display_mutation,\
                        fusion_gene1, fusion_gene2, exon1, exon2, chromosome1,\
                        chromosome2, transcript1, transcript2, fusion_source, fusion_type,\
                        pair_order]
    
    ####### 数据归一 ########
    # “基因组改变“信息
    df.loc[:, 'genomic_change'] = df.loc[:, 'genomic_change'].apply(deal_genomic_change)

    # DNA区域信息
    df.loc[df.loc[:, 'dna_region'] == '', 'dna_region'] = np.nan

    # 核苷酸改变
    df.loc[:, 'mutation_c'] = df.loc[:, 'mutation_c'].apply(deal_mutation_c)

    # 氨基酸改变
    df.loc[:, 'mutation_p'] = df.loc[:, 'mutation_p'].apply(deal_mutation_p)

    # 转录本号
    df.loc[df.loc[:, 'transcript'] == '', 'transcript'] = np.nan

    # sift功能预测分值
    df.loc[df.loc[:, 'sift_score'] == '', 'sift_score'] = np.nan
    df.loc[df.loc[:, 'sift_score'] == '.', 'sift_score'] = '0'
    # 保留两位小数
    df.loc[:, 'sift_score'] = df.loc[:, 'sift_score'].apply(keep_two_digit)

    # sift功能预测结果
    df.loc[df.loc[:, 'sift_predict'].isin(['.', '-']), 'sift_predict'] = np.nan

    # 功能改变类型


    # polyphen2预测结果
    df.loc[df.loc[:, 'polyphen2_predict'].isin(['.', '-']), 'polyphen2_predict'] = np.nan

    # 是否胚系
    df.loc[:, 'genomic_source'] = df.loc[:, 'genomic_source'].map({0:'体细胞', 1:'胚系'})

    # acmg
    
    # 等位基因状态
    df.loc[:, 'zygosity'] = df.loc[:, 'zygosity'].map({0:'杂合', 1:'纯合'})

    # chromosome
    df.loc[df.loc[:, 'chromosome'] == '7', 'chromosome'] = 'chr7'

    # ExAC_all
    df.loc[df.loc[:, 'exac_all'].isin(['.', '']), 'exac_all'] = np.nan
    df.loc[:, 'exac_all'] = df.loc[:, 'exac_all'].apply(as_num)

    # ExAC_EAS, gnomAD, 1000g
    df.loc[df.loc[:, 'exac_eas'] == '', 'exac_eas'] = np.nan
    df.loc[df.loc[:, 'genomad_genome_all'] == '', 'genomad_genome_all'] = np.nan
    df.loc[df.loc[:, 'genomad_genome_eas'] == '', 'genomad_genome_eas'] = np.nan
    df.loc[df.loc[:, '1000g2015aug_all'] == '', '1000g2015aug_all'] = np.nan
    df.loc[df.loc[:, '1000g2015aug_eas'] == '', '1000g2015aug_eas'] = np.nan




def keep_two_digit(ele):
    try:
        if np.isnan(ele):
            pass
    except:
        ele = "%.2f" % (float(ele))

    return ele

def as_num(ele):
    '科学计数法转化为小数'
    try:
        if np.isnan(ele):
            pass
    except:
        y = '%.20f'%(ele)
        x = float(y) # 删除多余的0
    return x

def deal_genomic_change(ele):
    pass


def deal_mutation_c(ele):
    pass


def deal_mutation_p(ele):
    pass


if __name__ == '__main__':
    main()