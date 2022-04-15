'''ngs实验相关信息清洗脚本，生成ngs_test_clean_final表，目前不能再运行，因为炙萍姐在生成表中增加了一列，再次运行会覆盖该表'''
import sys
from datetime import datetime
sys.path.append('/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗脚本')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_info


def main():
    # 获取ngs实验相关信息
    conn = get_database_conn()
    sql = '''SELECT
                r.report_number,
                r.published_at,
                r.pipeline_version,
                t.short_description,
                r.id,
                r.patient_id 
            FROM
                reports r
                INNER JOIN templates t ON r.template_id = t.id;'''
    result = query_database(conn, sql)

    sql_2 = '''SELECT DISTINCT
                    order_table_id 
                FROM
                    order_clean_final 
                WHERE
                    excluded_by_hospital = 0;'''
    result_2 = query_database(conn, sql_2)

    conn.close()

    # 排除订单信息
    order_table_id_exclude = [orders[0] for orders in result_2]

    # ngs实验相关信息处理
    # 订单信息处理
    result_new = []

    for tuple in result:
        tuple = [ele.strip() if isinstance(ele, str) else ele for ele in tuple]

        report_number = tuple[0]
        published_at = tuple[1]
        pipeline_version = tuple[2]
        report_type = tuple[3]
        report_id = tuple[4]
        order_table_id = tuple[5]

        nucleic_acid = None
        if report_type in ['百适博', '耀适博', '朗适博']:
            nucleic_acid = 'DNA'
        elif report_type == '容适博':
            nucleic_acid = 'RNA'
        else:
            pass

        detection_method = '目标基因捕获测序方法'
        reference_genome = 'GRCh37/hg19'

        

        # 建表时间
        created_at = datetime.now()
        lot_number = 0
        lot_date_range = '>=2019-06-01,<=2021-12-31'

        # 根据订单ID，判断是否需要排除
        excluded_by_hospital = 1
        if order_table_id in order_table_id_exclude:
            excluded_by_hospital = 0

        result_new.append([report_number, published_at, report_type, detection_method, nucleic_acid,
            reference_genome, pipeline_version, report_id, order_table_id, created_at,
            lot_number, lot_date_range, excluded_by_hospital])

    # 创建ngs实验信息表
    conn = get_database_conn()
    order_table_sql = '''create table ngs_test_clean_final (
        id int(10) unsigned not null,
        report_id int(10) unsigned not null comment 'reports表id',
        order_table_id int(10) unsigned not null comment 'patients表id',
        report_number varchar(255) not null comment '报告编号',
        published_at timestamp not null comment '报告日期',
        report_type varchar(255) not null comment '报告类型',
        detection_method varchar(255) not null comment '测序技术',
        nucleic_acid varchar(255) default null comment '核酸类型',
        reference_genome varchar(255) not null comment '参考基因组',
        pipeline_version varchar(255) not null comment '流程版本',
        excluded_by_hospital tinyint(4) not null comment '订单来源医院是否确为医院，0代表非医院，1代表医院',
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        primary key (id),
        unique (report_number),
        unique (report_id)
    )'''

    create_table(conn, 'ngs_test_clean_final', order_table_sql)
    conn.close()


    # 插入数据
    conn = get_database_conn()
    cur = conn.cursor()
    n = 0 
    for eles in result_new:
        n += 1
        info = f'''({n}, {eles[7]}, {eles[8]}, '{eles[0]}', '{eles[1].strftime('%Y-%m-%d %H:%M:%S')}', 
            '{eles[2]}', '{eles[3]}', '{eles[4]}', '{eles[5]}', '{eles[6]}',
            {eles[12]}, '{eles[9].strftime('%Y-%m-%d %H:%M:%S')}', {eles[10]}, '{eles[11]}')'''
        insert_sql = f'''insert into ngs_test_clean_final values {info}'''
        insert_sql = insert_sql.replace("'None',", "null,")
        insert_table_info(conn, cur, insert_sql)
    cur.close()
    conn.close()


if __name__ == '__main__':
    # main()