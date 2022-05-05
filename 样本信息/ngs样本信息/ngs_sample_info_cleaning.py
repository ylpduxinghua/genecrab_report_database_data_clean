import sys
sys.path.append('/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗脚本')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info
from datetime import datetime


def get_sample_name_mapping_dict(in_file):
    '样本名称对应关系'
    result = {}
    with open(in_file) as f:
        f.readline()
        for line in f:
            line_list = line.strip().split('\t')
            line_list = [None if ele == 'NULL' else ele for ele in line_list]
            result[line_list[0]] = line_list[1]

    return result


def get_sample_source_dict(in_file):
    '样本来源，转移部位，侧性信息'
    source_dict = {}
    with open(in_file) as f:
        f.readline()
        for line in f:
            line_list = line.rstrip().split('\t')
            line_list = [None if ele == 'NULL' else ele for ele in line_list]
            source_dict[line_list[0] + line_list[1]] = (line_list[2], line_list[3])
    return source_dict


def main():
    sample_name_mapping_file = '/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗规则/样本名称对应关系字典表/sample_name_dict.tsv'
    # 特定样本的样本名称信息
    sample_name_specific_sample = '/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗规则/样本名称对应关系字典表/sample_name_specific_samples.tsv'
    sample_name_mapping_dict = get_sample_name_mapping_dict(sample_name_mapping_file)
    sample_name_specific_sample_dict = get_sample_name_mapping_dict(sample_name_specific_sample)
    # 原发灶字典表
    yuanfazao_source_file = '/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗规则/转移部位/yuanfazao_dict_20220429.tsv'
    # 转移灶字典表
    zhuanyizao_source_file = '/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗规则/转移部位/zhuanyizao_dict_20220429.tsv'
    # 未知字典表
    weizhi_source_file = '/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗规则/转移部位/weizhi_dict_20220429.tsv'
    yuanfazao_dict = get_sample_source_dict(yuanfazao_source_file)
    zhuanyizao_dict = get_sample_source_dict(zhuanyizao_source_file)
    weizhi_dict = get_sample_source_dict(weizhi_source_file)

    # 获取原始样本信息
    conn = get_database_conn()
    sql = '''SELECT
                id,
                patient_id,
                report_id,
                sample_id,
                sample_short_id,
                sample_name,
                received_at,
                vitro_at,
                body_part_name,
                metastasis_target,
                panel_id 
            FROM
                report_samples;'''
    result = query_database(conn, sql)
    sql_panel = 'select * from dict_panel;'
    result_panel = query_database(conn, sql_panel)
    sql_sample_qc = 'select sample_id, created_at from sample_qc;'
    result_sample_qc = query_database(conn, sql_sample_qc)
    conn.close()

    # panel dict
    panel_dict = {}
    for eles in result_panel:
        panel_dict[eles[0]] = eles[1]
    # 样本分析日期信息
    sample_analysis_dict = {}
    for  eles in result_sample_qc:
        sample_analysis_dict[eles[0]] = eles[1]


    # 样本信息处理
    result_new = []
    for tuple in result:
        report_samples_table_id = tuple[0]
        order_table_id = tuple[1]
        reports_table_id = tuple[2]
        sample_id = tuple[3]
        sample_short_id = tuple[4]
        sample_name = tuple[5]
        received_at = tuple[6]
        vitro_at = tuple[7]
        body_part_name = tuple[8]
        metastasis_target = tuple[9]
        panel_id = tuple[10]

        # 根据sample_id排除样本，原因：同一sample_id对应不同的patient_id
        sample_id_excluded = 'R-190809-555248-PE-549910_PE-SQ_E19082304-19-DNA_F1908236707-3_L190823-00006-B5_20190825-C4_panel12_P190825876-0_sample20190825-1_1566901802000_1566901802000'
        exclude_by_sampleID = 1
        if sample_id == sample_id_excluded:
            exclude_by_sampleID = 0

        # sample_name 映射
        if sample_id in sample_name_specific_sample_dict:
            sample_name = sample_name_specific_sample_dict[sample_id]
        elif sample_name in sample_name_mapping_dict:
            sample_name = sample_name_mapping_dict[sample_name]
        else:
            print('sample_name error!')

        # 样本来源，样本解剖部位，样本侧性
        body_site_name = None
        laterality = None
        label = body_part_name + metastasis_target
        if label in yuanfazao_dict:
            body_site_name = yuanfazao_dict[label][0]
            laterality = yuanfazao_dict[label][1]
        elif label in zhuanyizao_dict:
            body_site_name = zhuanyizao_dict[label][0]
            laterality = zhuanyizao_dict[label][1]
        elif label in weizhi_dict:
            body_site_name = weizhi_dict[label][0]
            laterality = weizhi_dict[label][1]
        else:
            print('存在错误')

        # body_part_name 
        if body_part_name in ['', '未知', '未选择']:
            body_part_name = None

        # 分析日期
        analysis_time = None
        if sample_id in sample_analysis_dict:
            analysis_time = sample_analysis_dict[sample_id]

        # panel_name 
        panel_name = panel_dict[panel_id]

        # 建表时间 清洗批次信息 清洗数据的时间范围
        created_at = datetime.now()
        lot_number = 0
        lot_date_range = '>=2019-06-01,<=2021-12-31'


        # datatime 格式化
        received_at = received_at.strftime('%Y-%m-%d %H:%M:%S')
        vitro_at = vitro_at.strftime('%Y-%m-%d %H:%M:%S')
        if analysis_time is not None:
            analysis_time = analysis_time.strftime('%Y-%m-%d %H:%M:%S')
        created_at = created_at.strftime('%Y-%m-%d %H:%M:%S')
        tmp = [report_samples_table_id, reports_table_id, order_table_id, sample_id, sample_short_id,
            sample_name, body_part_name, body_site_name, laterality, received_at,
            vitro_at, analysis_time, panel_name, created_at, lot_number,
            lot_date_range, exclude_by_sampleID]

        tmp = [ele if ele is None else str(ele) for ele in tmp]

        result_new.append(tmp)


    # 创建ngs样本信息表
    conn = my_database_conn()
    ngs_sample_info_sql = '''create table ngs_sample_information_clean_final (
        id int(10) not null auto_increment,
        report_sample_table_id int(10) unsigned not null comment 'report_samples表id',
        reports_table_id int(10) unsigned not null comment 'reports表id',
        order_table_id int(10) unsigned not null comment 'patients表id',
        sample_id varchar(255) not null comment '样本唯一id',
        sample_short_id varchar(255) not null comment '样本短id',
        sample_name varchar(255) default null comment '样本名称',
        body_part_name varchar(255) default null comment '样本来源',
        body_site_name varchar(255) default null comment '样本解剖部位',
        laterality varchar(255) default null comment '样本侧性',
        received_at timestamp not null comment '收样日期',
        vitro_at timestamp not null comment '采样日期',
        analysis_time timestamp default null comment '分析日期',
        panel_name varchar(255) not null comment 'panel名称',
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        excluded_by_sampleID tinyint(4) default null comment '对应不同patient_id样本',
        primary key (id)
    )'''

    create_table(conn, 'ngs_sample_information_clean_final', ngs_sample_info_sql)
    conn.close()


    # 插入数据
    conn = my_database_conn()
    cur = conn.cursor()
    insert_sql = f'''insert into ngs_sample_information_clean_final (report_sample_table_id, reports_table_id, order_table_id, sample_id, sample_short_id,
            sample_name, body_part_name, body_site_name, laterality, received_at,
            vitro_at, analysis_time, panel_name, created_at, lot_number,
            lot_date_range, excluded_by_sampleID) values (%s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s)'''
    insert_table_many_info(conn, cur, insert_sql, result_new)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()









