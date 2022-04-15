'''本脚本用于清洗订单表，生成order_clean_final表，目前不能再运行，因为炙萍姐在生成表中增加了一列，再次运行会覆盖该表'''
import sys
sys.path.append('/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗脚本')
from mysql_module import get_database_conn, my_database_conn, query_database, get_hospital_mapping_dict, create_table, insert_table_info
from datetime import datetime



def main():
    # 获取原始订单信息
    conn = get_database_conn()
    sql = '''SELECT DISTINCT
                order_id,
                hospital,
                doctor,
                seller,
                product_type_name,
                submission_date,
                id,
                pid
            FROM
                patients;'''
    result = query_database(conn, sql)

    conn.close()
    
    # 订单信息处理
    result_new = []
    hospital_mapping_dict = get_hospital_mapping_dict()

    for tuple in result:
        tuple = [ele.strip() if isinstance(ele, str) else ele for ele in tuple]

        order_id = tuple[0]

        # 医院信息处理
        hospital = tuple[1]
        hospital_new = None
        excluded_by_hospital = 0
        if hospital in hospital_mapping_dict:
            hospital_new = hospital_mapping_dict[hospital]
            excluded_by_hospital = 1

        # 医生信息处理
        doctor = tuple[2]
        doctor_new = clean_doctor(doctor)

        seller = tuple[3]
        product_type_name = tuple[4]
        submission_date = tuple[5]

        order_table_id = tuple[6]
        pid = tuple[7]

        # 建表时间
        created_at = datetime.now()
        lot_number = 0
        lot_date_range = '>=2019-06-01,<=2021-12-31'

        tmp = [order_id, hospital_new, doctor_new, seller, product_type_name,
            submission_date, excluded_by_hospital, order_table_id, pid, created_at,
            lot_number, lot_date_range]
        result_new.append(tmp)

    # 创建订单表
    conn = get_database_conn()
    order_table_sql = '''create table order_clean_final (
        id int(10) unsigned not null,
        order_table_id int(10) unsigned not null comment '原始订单表id',
        order_id varchar(255) not null comment '订单编号',
        pid varchar(255) not null comment '患者pid',
        hospital varchar(255) default null comment "订单来源医院",
        doctor varchar(255) default null comment '医生',
        seller varchar(255) default null comment '销售',
        product_type_name varchar(255) not null comment '订单产品',
        order_date timestamp not null comment '订单日期',
        excluded_by_hospital tinyint(4) not null comment '订单来源医院是否确为医院，0代表非医院，1代表医院',
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        primary key (id),
        unique (order_table_id),
        unique (order_id)
    )'''

    create_table(conn, 'order_clean_final', order_table_sql)
    conn.close()

    # 插入订单数据
    conn = get_database_conn()
    cur = conn.cursor()
    n = 0 
    for eles in result_new:
        n += 1
        info = f'''({n}, {eles[7]}, '{eles[0]}', '{eles[8]}', '{eles[1]}', '{eles[2]}', '{eles[3]}', 
            '{eles[4]}', '{eles[5].strftime('%Y-%m-%d %H:%M:%S')}', {eles[6]}, 
            '{eles[9].strftime('%Y-%m-%d %H:%M:%S')}', {eles[10]}, '{eles[11]}')'''
        insert_sql = f'''insert into order_clean_final values {info}'''
        insert_sql = insert_sql.replace("'None',", "null,")
        insert_table_info(conn, cur, insert_sql)
    cur.close()
    conn.close()




def clean_doctor(doctor):
    # 如果doctor为 ''，映射为None
    if doctor == '':
        doctor = None
        return doctor
    # 带括号处理
    # 特殊字段映射
    mapping_fields = {
        '2胡卫东':None,
        '郭伟 张兴': None,
        '肿瘤内科': None,
        '大坪医院': None,
        '西安交通大学第一附属医院': None,
        '呼吸内科': None,
        '肿瘤外科': None,
        '（王怀碧）江飞龙': '王怀碧',
        '生物治疗科': None,
        '': None,
        '无': None,
        '梁': None,
        '岳': None,
        '王': None,
        '凌': None,
        '未知': None
    }
    if '（' in doctor and (not doctor in mapping_fields):
        doctor = doctor.split('（')[0]
        return doctor
    
    if doctor in mapping_fields:
        doctor = mapping_fields[doctor]
    return doctor


if __name__ == '__main__':
    # main()
    # query_doctor()