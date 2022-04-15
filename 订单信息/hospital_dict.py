import sys
from mysql_module import get_database_conn, create_table, insert_table_info


def get_dict_and_mapping_table(hospital_info_file):
    hospital_province_dict = {}
    hospital_mapping_dict = {}
    with open(hospital_info_file) as f:
        f.readline()
        for line in f:
            line_list = line.split('\t')
            hospital = line_list[4].strip()
            province = line_list[5].strip()
            hospital_label = line_list[7].strip()
            if hospital_label == '非医院':
                continue
            hospital_province_dict[hospital] = province

            hospital_raw = line_list[1].strip()
            hospital_mapping_dict[hospital_raw] = hospital

    return hospital_province_dict, hospital_mapping_dict


def create_hospital_table():
    conn = get_database_conn()

    hospital_mapping_table_sql = f'''create table  mapping_hospital_to_std(
        id int(11) not null,
        hospital varchar(255) default null comment '初始医院信息',
        hospital_std varchar(255) default null comment '标准化医院信息',
        primary key (id)
    )'''
    hospital_dict_table_sql = f'''create table  dict_hospital(
        id int(11) not null,
        hospital varchar(255) default null comment '医院信息',
        province varchar(255) default null comment '医院所在省份信息',
        primary key (id)
    )'''

    # 创建mapping表
    create_table(conn, 'mapping_hospital_to_std', hospital_mapping_table_sql)
    # 创建字典表
    create_table(conn, 'dict_hospital', hospital_dict_table_sql)

    conn.close()


def insert_hospital_info(hospital_province_dict, hospital_mapping_dict):
    conn = get_database_conn()
    cur = conn.cursor()

    n = 0
    for hospital in hospital_mapping_dict:
        hospital_str = hospital_mapping_dict[hospital]
        n += 1
        info = f"({n}, '{hospital}', '{hospital_str}')"
        insert_sql = f'''insert into mapping_hospital_to_std (id, hospital, hospital_std) values {info}'''
        insert_table_info(conn, cur, insert_sql)

    m = 0
    for hospital in hospital_province_dict:
        province = hospital_province_dict[hospital]
        m += 1
        info = f"({m}, '{hospital}', '{province}')"
        insert_sql = f'''insert into dict_hospital (id, hospital, province) values {info}'''
        insert_table_info(conn, cur, insert_sql)
    
    cur.close()
    conn.close()


def main():
    hospital_info_file = '/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗规则/医院标准化-2/医院字典表构建_v1_20220411.tsv'
    # hospital_dict_file = sys.argv[2]
    # hospital_mapping_file = sys.argv[3]

    # 获取医院相关信息
    hospital_province_dict, hospital_mapping_dict = get_dict_and_mapping_table(hospital_info_file)

    # 创建表
    create_hospital_table()

    # 向创建表插入信息
    insert_hospital_info(hospital_province_dict, hospital_mapping_dict)


if __name__ == '__main__':
    main()
    # get_database_conn()

