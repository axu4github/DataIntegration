# coding=utf-8

import click
import MySQLdb
from prettytable import PrettyTable
import time
import os
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

""" 数据整合运维脚本 """

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TASK_DETAIL_HEADERS = ["任务编号",
                       "步骤",
                       # "步骤索引",
                       # "查询日期",
                       "查询开始时间",
                       "查询结束时间",
                       "步骤执行结束时间"]

STEP_CODE_TO_NAME = {
    "voice_validate": "索引校验",
    "voice_transform": "索引转换和扩展",
    "voice_hbase": "索引存储到HBase",
    "voice_solr": "索引存储到Solr",
    "voice_hive": "索引存储到Hive",
    "voicefile_validate": "语音校验",
    "voicefile_ftp": "语音下载（FTP）",
    "voicefile_waveform": "语音转波形图",
    "voicefile_stt": "语音识别",
}

# click 模块配置
CLICK_CONTEXT_SETTINGS = dict(
    help_option_names=["-h", "--help"],
    terminal_width=100)
CLICK_COLOR_INFO = "green"
CLICK_COLOR_ERROR = "red"

MYSQL_HOST = "10.0.3.45"
MYSQL_USER = "root"
MYSQL_PASSWD = "root123"
MYSQL_DB = "operations"

SQL = """
    SELECT
        *
    FROM
        steps s
    WHERE
        s.date = "{date}";
"""

conn = MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER,
                       passwd=MYSQL_PASSWD, db=MYSQL_DB,
                       connect_timeout=5)  # 超时（单位：秒）
cur = conn.cursor()


def sqlquery(sql, cur=None):
    cur.execute(sql)
    return [row for row in cur]


def groupby_row(rows):
    """ 按照id对查询出的行进行分组 """
    r = {}
    for row in rows:
        _id = row[1]
        if _id not in r:
            r[_id] = [row]
        else:
            r[_id].append(row)
    return r


def transform_step(code):
    name = code
    if code in STEP_CODE_TO_NAME:
        name = STEP_CODE_TO_NAME[code]
    return name


@click.group(context_settings=CLICK_CONTEXT_SETTINGS)
@click.help_option("-h", "--help", help="使用说明")
def cli():
    """
    数据整合运维脚本

    获取帮助信息请执行：python operations.py --help
    """
    pass


@cli.command(short_help="查看任务执行情况")
@click.option("--date",
              default=time.strftime("%Y%m%d", time.localtime()),
              help="归集日期")
def task(date):
    execute_sql = SQL.format(date=date).strip()
    # click.echo(execute_sql)
    tasks = groupby_row(sqlquery(execute_sql, cur))
    if len(tasks) > 0:
        table = PrettyTable()
        table.field_names = TASK_DETAIL_HEADERS
        for (_id, steps) in tasks.items():
            for step in steps:
                step = list(step)
                del step[3]  # 删除 步骤索引 列
                del step[3]  # 删除 查询日期 列
                step[2] = transform_step(step[2])
                table.add_row(step[1:])

        click.echo(table)


if __name__ == "__main__":
    cli()
