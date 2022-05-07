import sys
sys.path.append('/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗脚本')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info
from datetime import datetime
import pandas as pd
import numpy as np



def deal_contamination_ratio(ele):
    if ele in ['null', '-']:
        ele = np.nan
    elif '%' in ele:
        pass
    else:
        ele =  "%.2f%%" % (float(ele) * 100)
    return ele



def main():
    # 获取原始样本QC信息
    conn = get_database_conn()
    sql = '''SELECT
                report_id,
                sample_id,
                qc_name,
                qc_value,
                qc_result
            FROM
                report_sample_qc_data_copy1 UNION
            SELECT
                report_id,
                sample_id,
                qc_name,
                qc_value,
                qc_result
            FROM
                rpt3_sample_qc_data;'''
    result = query_database(conn, sql)
    conn.close()


    # 创建存储数据的dataframe
    columns_list = ['report_id', 'sample_id', 'tumor_cell_proportion', 'total_extraction_amount', 'total_library_amount', 'initial_library_amount', 'dna_volume',
                    'dna_concentration', 'mean_depth', 'median_depth', 'insert_size', 'mapping_ratio', 
                    'dedup_mean_depth', 'dedup_median_depth', 'uniformity', 'concordance', 'nt_percent', 
                    'primates_percent', 'bacteria_percent', 'viruses_percent', 'isTopRight', 'topunexpect',
                    'contamination_assessment', 'contamination_ratio', 'q30_ratio', 'clean_q30_ratio', 'hemolysis_assessment',
                    'volume_for_extraction', 'hk_num', 'target_reads', 'positive_reference', 'negative_reference',
                    'blank_reference', 'overall_quality']

    df = pd.DataFrame(data=None, columns=columns_list)

    # 样本QC信息整合
    columns_mapping_dict = {'肿瘤细胞占比':'tumor_cell_proportion', '提取总量（ng）':'total_extraction_amount', '文库总量（ng）':'total_library_amount', 
                            '建库起始量':'initial_library_amount', 'DNA体积':'dna_volume', 'DNA浓度':'dna_concentration', 
                            '平均测序深度':'mean_depth', '中位测序深度':'median_depth', '插入片段长度（bp）':'insert_size', 
                            '序列回帖比率':'mapping_ratio', '去重后的平均深度':'dedup_mean_depth', '去重后测序深度中位数':'dedup_median_depth', 
                            '测序深度均一性':'uniformity', '配对样本相关性':'concordance', 'nt_percent':'nt_percent', 
                            'primates_percent':'primates_percent', 'bacteria_percent':'bacteria_percent', 'viruses_percent':'viruses_percent', 
                            'isTopRight':'isTopRight', 'topUnexpect':'topUnexpect', '污染比对是否合格':'contamination_assessment', 
                            'Contamination':'contamination_ratio', 'q30_ratio':'q30_ratio', 'clean_Q30_ratio':'clean_q30_ratio', 
                            '是否溶血':'hemolysis_assessment', '提取量':'volume_for_extraction', 'hk_num':'hk_num', 
                            'target_reads':'target_reads', '阳性参考品':'positive_reference', '阴性参考品':'negative_reference',
                            '空白参考品':'blank_reference'}

    for tuple in result:
        report_id = tuple[0]
        sample_id = tuple[1]
        qc_name = tuple[2]
        qc_value = tuple[3]
        qc_result = tuple[4]

        df.loc[sample_id + '_' + str(report_id), 'report_id'] = report_id
        df.loc[sample_id + '_' + str(report_id), 'sample_id'] = sample_id
        df.loc[sample_id + '_' + str(report_id), 'overall_quality'] = qc_result
        if qc_name in columns_mapping_dict:
            df.loc[sample_id + '_' + str(report_id), columns_mapping_dict[qc_name]] = qc_value
        

    df.to_csv('ngs_sample_qc.tsv', sep='\t')

    # 将数据库中的NaN和None替换为'None', 目的是为了方便正则匹配
    # df[pd.isna(df)] = None

    ############## 样本QC信息归一 #########
    # 肿瘤细胞占比
    df.loc[:, 'tumor_cell_proportion'][(df.loc[:,'tumor_cell_proportion'] == '无法判读') | 
                                        (df.loc[:,'tumor_cell_proportion'] == '无法判读-组织前处理不佳')|
                                        (df.loc[:,'tumor_cell_proportion'] == 'N/A')|
                                        (df.loc[:,'tumor_cell_proportion'] == '')] = np.nan
    df.loc[:, 'tumor_cell_proportion'][df.loc[:,'tumor_cell_proportion'] == '未见肿瘤细胞'] = '0%'


    # 配对样本一致性
    df.loc[:, 'concordance'][(df.loc[:,'concordance'] == 'A') | (df.loc[:,'concordance'] == 'B')| (df.loc[:,'concordance'] == 'C')] = np.nan


    # 污染比例
    df.loc[:, 'contamination_ratio'] = df.loc[:, 'contamination_ratio'].apply(deal_contamination_ratio)

    # 

if __name__ == '__main__':
    main()

