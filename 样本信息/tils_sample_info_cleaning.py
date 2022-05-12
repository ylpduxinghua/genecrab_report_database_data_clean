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


def main():
    sample_name_mapping_file = '/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗规则/样本名称对应关系字典表/sample_name_dict.tsv'
    # 特定样本的样本名称信息
    sample_name_specific_sample = '/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗规则/样本名称对应关系字典表/sample_name_specific_samples.tsv'
    sample_name_mapping_dict = get_sample_name_mapping_dict(sample_name_mapping_file)
    sample_name_specific_sample_dict = get_sample_name_mapping_dict(sample_name_specific_sample)
    
    # 获取原始样本信息
    conn = get_database_conn()
    sql = '''SELECT
                id,
                patient_id,
                report_id,
                sample_id,
                sample_name,
                received_at,
                vitro_at,
                tumor_cd8,
                tumor_pd1,
                tumor_cd8_pd1,
                tumor_cd68,
                tumor_cd68_pdl1 
            FROM
                report_tils;'''
    result = query_database(conn, sql)
    conn.close()

    # 样本信息处理
    result_new = []
    for tuple in result:
        report_tils_table_id = tuple[0]
        order_table_id = tuple[1]
        reports_table_id = tuple[2]
        sample_id = tuple[3]
        sample_name = tuple[4].strip()
        received_at = tuple[5]
        vitro_at = tuple[6]
        tumor_cd8 = tuple[7]
        tumor_pd1 = tuple[8]
        tumor_cd8_pd1 = tuple[9]
        tumor_cd68 = tuple[10]
        tumor_cd68_pdl1 = tuple[11]

        # sample_name 映射
        if sample_id in sample_name_specific_sample_dict:
            sample_name = sample_name_specific_sample_dict[sample_id]
        elif sample_name in sample_name_mapping_dict:
            sample_name = sample_name_mapping_dict[sample_name]
        else:
            print(sample_name)
            print('sample_name error!')

        # 结果处理: 有一条结果为空
        if tumor_cd8 == '':
            tumor_cd8 = None
            tumor_pd1 = None
            tumor_cd8_pd1 = None
            tumor_cd68 = None
            tumor_cd68_pdl1 = None

        # 建表时间 清洗批次信息 清洗数据的时间范围
        created_at = datetime.now()
        lot_number = 0
        lot_date_range = '>=2019-06-01,<=2021-12-31'

        # datatime 格式化
        received_at = received_at.strftime('%Y-%m-%d %H:%M:%S')
        vitro_at = vitro_at.strftime('%Y-%m-%d %H:%M:%S')
        created_at = created_at.strftime('%Y-%m-%d %H:%M:%S')

        tmp = [report_tils_table_id, reports_table_id, order_table_id, sample_id, sample_name,
            received_at, vitro_at, tumor_cd8, tumor_pd1, tumor_cd8_pd1,
            tumor_cd68, tumor_cd68_pdl1, created_at, lot_number, lot_date_range]
        
        tmp = [ele if ele is None else str(ele) for ele in tmp]

        result_new.append(tmp)

    # 创建ngs样本信息表
    conn = get_database_conn()
    tils_sample_info_sql = '''create table tils_sample_information_clean_final (
        id int(10) not null auto_increment,
        report_tils_table_id int(10) unsigned not null comment 'report_tils表id',
        reports_table_id int(10) unsigned not null comment 'reports表id',
        order_table_id int(10) unsigned not null comment 'patients表id',
        sample_id varchar(255) not null comment '样本唯一id',
        sample_name varchar(255) default null comment '样本名称',
        received_at timestamp not null comment '收样日期',
        vitro_at timestamp not null comment '采样日期',
        tumor_cd8 float default null comment 'cd8阳性T细胞比例',
        tumor_pd1 float default null comment 'cd8阳性T细胞比例',
        tumor_cd8_pd1 float default null comment 'cd8阳性T细胞比例',
        tumor_cd68 float default null comment 'cd8阳性T细胞比例',
        tumor_cd68_pdl1 float default null comment 'cd8阳性T细胞比例',
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        primary key (id)
    )'''

    create_table(conn, 'tils_sample_information_clean_final', tils_sample_info_sql)
    conn.close()

    # 插入数据
    conn = get_database_conn()
    cur = conn.cursor()
    insert_sql = f'''insert into tils_sample_information_clean_final (report_tils_table_id, reports_table_id, order_table_id, sample_id, sample_name,
            received_at, vitro_at, tumor_cd8, tumor_pd1, tumor_cd8_pd1,
            tumor_cd68, tumor_cd68_pdl1, created_at, lot_number, lot_date_range) values (%s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s)'''
    insert_table_many_info(conn, cur, insert_sql, result_new)
    cur.close()
    conn.close()



if __name__ == '__main__':
    main()