import sys
sys.path.append('/duxinghua/genecrab_report_database_data_clean')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info
from datetime import datetime
import pandas as pd
import numpy as np



def deal_contamination_ratio(ele):
    try:
        if np.isnan(ele):
            pass
    except:
        if ele in ['null', '-']:
            ele = np.nan
        elif '%' in ele:
            ele = ele.split('%')[0]
            ele = "%.2f" % (float(ele))
        else:
            ele =  "%.2f" % (float(ele) * 100)
    return ele


def deal_percent_ratio(ele):
    try:
        if np.isnan(ele):
            pass
    except:
        ele = ele.split('%')[0]
        ele = "%.2f" % (float(ele))

    return ele


def main():
    # 获取原始样本QC信息
    conn = get_database_conn()
    sql = '''SELECT
                report_id,
                sample_id,
                qc_name,
                qc_value,
                qc_result,
                sample_name
            FROM
                report_sample_qc_data_copy1 UNION
            SELECT
                report_id,
                sample_id,
                qc_name,
                qc_value,
                qc_result,
                sample_name
            FROM
                rpt3_sample_qc_data;'''
    result = query_database(conn, sql)
    conn.close()


    # 创建存储数据的dataframe
    columns_list = ['report_id', 'sample_id', 'tumor_cell_proportion', 'total_extraction_amount', 'total_library_amount', 'initial_library_amount', 'dna_volume',
                    'dna_concentration', 'mean_depth', 'median_depth', 'insert_size', 'mapping_ratio', 
                    'dedup_mean_depth', 'dedup_median_depth', 'uniformity', 'concordance', 'nt_percent', 
                    'primates_percent', 'bacteria_percent', 'viruses_percent', 'isTopRight', 'topUnexpect',
                    'contamination_assessment', 'contamination_ratio', 'q30_ratio', 'clean_q30_ratio', 'hemolysis_assessment',
                    'volume_for_extraction', 'hk_num', 'target_reads', 'positive_reference', 'negative_reference',
                    'blank_reference', 'overall_quality', 'sample_name']

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
        sample_name = tuple[5]

        df.loc[sample_id + '_' + str(report_id), 'report_id'] = report_id
        df.loc[sample_id + '_' + str(report_id), 'sample_id'] = sample_id
        df.loc[sample_id + '_' + str(report_id), 'overall_quality'] = qc_result
        df.loc[sample_id + '_' + str(report_id), 'sample_name'] = sample_name
        if qc_name in columns_mapping_dict:
            df.loc[sample_id + '_' + str(report_id), columns_mapping_dict[qc_name]] = qc_value

    df.to_csv('ngs_sample_qc.tsv', sep='\t')

    # infile = sys.argv[1]
    # df = pd.read_csv(infile, sep='\t', index_col=0)
    # df.loc[:, 'sample_name'] = 'BC'

    ############## 样本QC信息归一 #########
    # 肿瘤细胞占比
    df.loc[:, 'tumor_cell_proportion'][(df.loc[:,'tumor_cell_proportion'] == '无法判读') | 
                                        (df.loc[:,'tumor_cell_proportion'] == '无法判读-组织前处理不佳')|
                                        (df.loc[:,'tumor_cell_proportion'] == 'N/A')|
                                        (df.loc[:,'tumor_cell_proportion'] == '')] = np.nan
    df.loc[:, 'tumor_cell_proportion'][df.loc[:,'tumor_cell_proportion'] == '未见肿瘤细胞'] = '0%'

    # 序列回帖比率
    df.loc[:, 'mapping_ratio'] = df.loc[:, 'mapping_ratio'].apply(deal_percent_ratio)

    # 细菌、病毒核酸占比
    df.loc[:, 'bacteria_percent'][df.loc[:, 'bacteria_percent'] == '.'] = '0'
    df.loc[:, 'viruses_percent'][df.loc[:, 'viruses_percent'] == '.'] = '0'

    # 配对样本一致性
    df.loc[:, 'concordance'][(df.loc[:,'concordance'] == 'A') | (df.loc[:,'concordance'] == 'B')| (df.loc[:,'concordance'] == 'C')] = np.nan

    # 污染比例
    df.loc[:, 'contamination_ratio'] = df.loc[:, 'contamination_ratio'].apply(deal_contamination_ratio)

    # q30_ratio, clean_Q30_ratio
    df.loc[:, 'q30_ratio'] = df.loc[:, 'q30_ratio'].apply(deal_percent_ratio)
    df.loc[:, 'clean_q30_ratio'] = df.loc[:, 'clean_q30_ratio'].apply(deal_percent_ratio)

    # 提取量
    selected_row_volume_for_extraction = df.loc[:, 'sample_name'].isin(['BC', 'Blood cell', '血细胞', 'plasma', '血浆', '脑脊液上清', '胸水上清'])
    df.loc[~selected_row_volume_for_extraction, 'volume_for_extraction'] = np.nan

    # 建表时间 清洗批次信息 清洗数据的时间范围
    df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['lot_number'] = 0
    df['lot_date_range'] = '>=2019-06-01,<=2021-12-31'

    # 删除sample_name列
    df.drop(['sample_name'], axis=1, inplace=True)

    # nan转化为'None'
    # df.fillna(None, inplace=True)
    df = df.where(~df.isna(), None)

    # dataframe转化为list
    result = df.values.tolist()

    # 数值转化为str
    result_new = []
    for tmp in result:
        result_new.append([ele if ele is None else str(ele) for ele in tmp])

    # 创建样本信息QC表
    conn = get_database_conn()
    ngs_sample_qc_sql = '''create table ngs_sample_qc_clean_final (
        id int(10) not null auto_increment,
        reports_table_id int(10) unsigned not null comment 'reports表id',
        sample_id varchar(255) not null comment '样本唯一id',
        tumor_cell_proportion varchar(255) default null comment '肿瘤细胞占比',
        total_extraction_amount decimal(8,2) default null comment '提取总量（ng）',
        total_library_amount decimal(8, 3) default null comment '文库总量（ng）',
        initial_library_amount decimal(10, 4) default null comment '建库起始量（ng）',
        dna_volume decimal(8, 3) default null comment 'DNA体积(ul)',
        dna_concentration decimal(8,4) default null comment 'DNA浓度(ng/ul)',
        mean_depth int(10) default null comment '平均测序深度',
        median_depth int(10) default null comment '中位测序深度',
        insert_size smallint default null comment '插入片段长度(bp)',
        mapping_ratio varchar(255) default null comment '序列回帖比率',
        dedup_mean_depth int(10) default null comment '去重后平均测序深度',
        dedup_median_depth int(10) default null comment '去重后中位测序深度',
        uniformity decimal(6, 2) default null comment '测序深度均一性',
        concordance varchar(20) default null comment '配对样本一致性',
        nt_percent decimal(6, 2) default null comment '核酸占比%',
        primates_percent decimal(6, 2) default null comment '灵长类核酸占比%',
        bacteria_percent decimal(6, 2) default null comment '细菌核酸占比%',
        viruses_percent decimal(6, 2) default null comment '病毒类核酸占比%',
        isTopRight varchar(20) default null comment '外源污染源判断',
        topUnexpect varchar(255) default null comment '外源物种taxon id',
        contamination_assessment varchar(20) default null comment '污染比对是否合格',
        contamination_ratio varchar(255) default null comment '污染比例',
        q30_ratio decimal(6, 2) default null comment 'Q30比例',
        clean_q30_ratio decimal(6, 2) default null comment 'Clean Q30比例%',
        hemolysis_assessment varchar(20) default null comment '样本是否溶血',
        volume_for_extraction decimal(6, 2) default null comment '提取用量',
        hk_num int(10) default null comment '管家基因数量',
        target_reads int(10) default null comment '目标区域reads数',
        positive_reference varchar(20) default null comment '阳性参考品',
        negative_reference varchar(20) default null comment '阴性参考品',
        blank_reference varchar(20) default null comment '空白参考品',
        overall_quality varchar(20) default null comment '样本总体质量',
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        primary key (id)
    )'''

    create_table(conn, 'ngs_sample_qc_clean_final', ngs_sample_qc_sql)
    conn.close()


    # 插入数据
    conn = get_database_conn()
    cur = conn.cursor()
    insert_sql = f'''insert into ngs_sample_qc_clean_final (reports_table_id, sample_id, tumor_cell_proportion, total_extraction_amount, total_library_amount,
        initial_library_amount, dna_volume, dna_concentration, mean_depth, median_depth,
        insert_size, mapping_ratio, dedup_mean_depth, dedup_median_depth, uniformity,
        concordance, nt_percent, primates_percent, bacteria_percent, viruses_percent,
        isTopRight, topUnexpect, contamination_assessment, contamination_ratio, q30_ratio,
        clean_q30_ratio, hemolysis_assessment, volume_for_extraction, hk_num, target_reads,
        positive_reference, negative_reference, blank_reference, overall_quality, created_at,
        lot_number, lot_date_range) values (%s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s)'''
    insert_table_many_info(conn, cur, insert_sql, result_new)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()

