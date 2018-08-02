# coding=utf-8

import os


class Config(object):

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    THIRD_PARTY_DIR = os.path.join(BASE_DIR, "thrid_party")
    TESTS_DIR = os.path.join(BASE_DIR, "tests")
    LOGS_DIR = os.path.join(BASE_DIR, "logs")

    if not os.path.exists(LOGS_DIR) or not os.path.isdir(LOGS_DIR):
        os.makedirs(LOGS_DIR)  # 创建日志目录

    ERROR_SEPARATOR = " && "
    ERROR_MESSAGE_SEPARATOR = " & "

    MULTIPLE_FIELDS = [
        "data_status_2",  # product_name
        "data_status_3",  # policy_code
        "satisfaction",  # polapplyage
        "summarize_content",  # newpaymode
        "region",  # way_name
        "data_status",  # pay_mode
        "group_leader",  # single_mode
        "isnormality",  # organ_area
        "to_improvement",  # sl_flag
        "total_prem",  # total_prem
        "charge_year",  # charge_year
        "agent_name",  # agent_name
        "ph_mobile",  # ph_mobile
    ]

    ALTER_HIVE_PARTITION_JAR_PATH = os.path.join(
        THIRD_PARTY_DIR, "alterHivePartition.jar")

    HIVE_CONFS = {
        "host": "10.163.89.72",
        "port": "10000",
        "db": "hengdaproject",
    }

    HIVE_TABLE_PATTERN_FIELD = "recorddate"
    HIVE_JDBC_URI = "jdbc:hive2://{host}:{port}/{db}".format(**HIVE_CONFS)
    HIVE_TABLE = "hengda_project_voice_smartv_hive_main"
    HIVE_TABLE_LOCATION = "/user/hive/warehouse/%s.db/%s/{%s}" % (
        HIVE_CONFS["db"], HIVE_TABLE, HIVE_TABLE_PATTERN_FIELD)

    HIVE_CREATE_PARTITIONS_SQL = "ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s='{%s}') LOCATION '%s'" % (
        HIVE_TABLE,
        HIVE_TABLE_PATTERN_FIELD,
        HIVE_TABLE_PATTERN_FIELD,
        HIVE_TABLE_LOCATION)

    CREATE_HIVE_PARTITIONS_COMMAND_PATTERN = "java -jar %s \"%s\" \"%s\"" % (
        ALTER_HIVE_PARTITION_JAR_PATH,
        HIVE_JDBC_URI,
        HIVE_CREATE_PARTITIONS_SQL)

    DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    BACKUP_DIR_DATE_FORMAT = "%Y%m%d_%H%M%S"

    WAV_TO_PNG_PATH = os.path.join(THIRD_PARTY_DIR, "wav2png", "wav2png")
    WAV_TO_PNG_COMMAND = "%s {input_dir} {output_dir} %s" % (
        WAV_TO_PNG_PATH, LOGS_DIR)

    # FTP 配置
    FTP_HOST = "10.163.91.122"
    FTP_USER = "record"
    FTP_PASS = "letmein"
    FTP_DOWNLOAD_ROOT_DIR = "/mnt/mfs/ftp_downloads"

    # FTP 下载失败重试次数
    DEFAULT_RETRY_NUMBER = 3

    # 中转服务器挂载目录
    DATA_SOURCE_DIR = ""
    # 中转服务器FTP目录
    FTP_DIR = ""

    # 语音文件转波形图，图片存储跟目录
    WAVFORM_ROOT_DIR = "/mnt/mfs/wavforms"
    # 波形图前缀
    WAVFORM_PREFIX = "data:image/png;base64,"

    # 恒大配置文件
    HD_MAPPING_FEILDS = {
        "area_of_job": "area_of_job",
        "objectivetype": "type_of_service",
        "starttime": "start_time",
        "endtime": "end_time",
        "stime": "duration",
        "agent_id": "rep_no",
        "rolegroupdescription": "rep_group",
        "customer_id": "account_no",
        "calltype": "calltype",
        "recordno": "number_of_record",
        "customername": "cust_name",
        "customerid": "cust_id",
        "customerlevel": "rule_prompt",
        "workorderid": "staff_name",
        "dialect_flag": "verify_type",
        "satisfaction": "rep_team",
        "vip_flag": "is_qa",
        "project_id": "phone_type",
        "attacheddata": "audit_no",
        "documentpath": "documentpath",
        "ani": "summarize_project",
        "dnis": "summarize_class",
        "extension": "qa_no",
        "staffname": "project_list",
        "gender": "to_warehouse",
        "objectivetypeid": "objectivetypeid",
        "objectivelevel": "objectivelevel",
        "staff_id": "staff_id",
        "rolegroup_id": "rolegroup_id",
        "record_guid": "record_guid",
        "event_guid": "event_guid",
        "objective_guid": "bu_type",
        "con": "con",
        "typeidinfo": "typeidinfo",
        "typeinfo": "typeinfo",
        "product_name": "data_status_2",
        "policy_code": "data_status_3",
        "polapplyage": "satisfaction",
        "newpaymode": "summarize_content",
        "way_name": "region",
        "pay_mode": "data_status",
        "single_mode": "group_leader",
        "organ_area": "isnormality",
        "sl_flag": "to_improvement",
        "total_prem": "total_prem",
        "charge_year": "charge_year",
        "agent_name": "agent_name",
        "ph_mobile": "ph_mobile",
        "customer_guid": "customer_guid",
        "idtype": "idtype",
        "birthday": "birthday",
        "mobile": "mobile",
        "tel_1": "tel_1",
        "tel_2": "tel_2",
        "tel_others": "tel_others",
        "address": "address",
        "workorder_guid": "workorder_guid",
        "workorderframe_id": "workorderframe_id",
        "workorderstatus": "workorderstatus",
        "hf_type": "hf_type",
        "hf_result": "hf_result",
        "createdby": "createdby",
        "createdgroup": "createdgroup",
        "createddate": "createddate",
        "modifieddate": "modifieddate",
        "workinfo": "workinfo",
        "callinfo": "callinfo",
        "mpolicy": "mapfilepath"
    }

    # 存储文件名字段
    HD_FILENAME_FIELD = "DOCUMENTPATH"

    # 存储语音识别结果根目录
    HD_STTR_BASE_DIR = "/mnt/mfs/voice_recognize_results"
    # 存储语音识别结果分目录
    HD_STTR_DIRS = {
        "speed": os.path.join(HD_STTR_BASE_DIR, "voiceresult"),
        "interrupt": os.path.join(HD_STTR_BASE_DIR, "voiceconflict"),
        "blankinfo": os.path.join(HD_STTR_BASE_DIR, "voicesence")
    }

    # 语音识别相关配置
    SPEECH_RECOGNITION_RESULT_DIR = HD_STTR_BASE_DIR
    SPEECH_RECOGNITION_DIR = "/root/hengda-release"
    SPEECH_RECOGNITION_CMD = "cd %s; %s/voice_rec %s/voice_recognize.ini {0}" % tuple(
        [SPEECH_RECOGNITION_DIR] * 3)

    # 语音识别集群处理时，默认分组（应等于语音识别集群服务器数量）
    DEFAULT_SPLIT_NUMBER = 1




if __name__ == "__main__":
    print(Config.WAV_TO_PNG_COMMAND)
