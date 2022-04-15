'''本脚本是在炙萍姐增加了excluded_new后， 修改pipeline_version字段'''
import sys
from datetime import datetime
sys.path.append('/Users/duxinghua/臻和工作/数据融合项目/肺癌商检项目/数据清洗脚本')
from mysql_module import get_database_conn, my_database_conn, query_database, create_table, insert_table_many_info


def main():
    # 获取ngs实验相关信息
    conn = get_database_conn()
    sql = '''SELECT
                *
            FROM
                ngs_test_clean_final;'''

    # sql = '''select * from ngs_test_clean_final where nucleic_acid is null limit 10;'''
    result = query_database(conn, sql)
    conn.close()

    # pipline版本修改
    result_new = []
    for tuple in result:
        line_list = [ele for ele in tuple]
        # pipline版本修改
        pipeline_version = line_list[-6]
        if not pipeline_version.startswith('v'):
            pipeline_version = None
        line_list[-6] = pipeline_version
        line_list[-4] = line_list[-4].strftime('%Y-%m-%d %H:%M:%S')
        line_list[4] = line_list[4].strftime('%Y-%m-%d %H:%M:%S')

        line_list = [ele if ele is None else str(ele) for ele in line_list]
        result_new.append(line_list)

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
        pipeline_version varchar(255) default null comment '流程版本',
        excluded_by_hospital tinyint(4) not null comment '订单来源医院是否确为医院，0代表非医院，1代表医院',
        created_at timestamp not null comment '建表日期',
        lot_number tinyint(4) not null comment '清洗批次信息',
        lot_date_range varchar(50) not null comment '清洗数据的时间范围',
        excluded_new tinyint(4) default null,
        primary key (id),
        unique (report_number),
        unique (report_id)
    )'''

    create_table(conn, 'ngs_test_clean_final', order_table_sql)
    conn.close()




    # 插入数据
    conn = get_database_conn()
    cur = conn.cursor()
    insert_sql = f'''insert into ngs_test_clean_final values (%s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s)'''
    insert_table_many_info(conn, cur, insert_sql, result_new)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()