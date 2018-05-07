# coding=utf-8

from processor import Processor, ProcessorResponse
from utils import Utils
from fields.base_field import BaseField
from fields.hd_field import HDField
from indexs.base_index import BaseIndex
from indexs.hd_index import HDIndex
from stt_parsers.tencent_stt_parser import TencentSTTParser
from stt_parsers.thinkit_stt_parser import ThinkitSTTParser
from confs.configs import Config
import unittest
import json
import datetime
import copy
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')


class TestProcessorResponse(unittest.TestCase):

    def test_response(self):
        self.assertEqual(ProcessorResponse()._print(),
                         (json.dumps(None), json.dumps(None), None))
        self.assertEqual(
            ProcessorResponse(corrects={"a": 1})._print(),
            (json.dumps({"a": 1}), json.dumps(None), None))
        self.assertEqual(
            ProcessorResponse(incorrects={"a": 1})._print(),
            (json.dumps(None), json.dumps({"a": 1}), None))
        self.assertEqual(
            ProcessorResponse(attributes={"a": 1})._print(),
            (json.dumps(None), json.dumps(None), {"a": 1}))


class TestProcessor(unittest.TestCase):
    """ 测试处理类 """

    def setUp(self):
        self.mapping_fields = {
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
            "callinfo": "callinfo"
        }

        self.mapping_data = """
            {
                "AREA_OF_JOB": "kf1",
                "PROJECT_ID": "-    ",
                "ATTACHEDDATA": "-",
                "EVENT_GUID": "---------",
                "RECORD_GUID": "e082f444-6f9a-433e-b5ec-1e06e754f0a0",
                "OBJECTIVETYPEID": "-",
                "OBJECTIVETYPE": "-",
                "OBJECTIVELEVEL": "99",
                "OBJECTIVE_GUID": null,
                "CON": "1",
                "TYPEIDINFO": "-",
                "TYPEINFO": "-",
                "WORKINFO": null,
                "RECORDNO": "-",
                "DOCUMENTPATH": "ftp://-/2018_04_03/20180403.wav",
                "CALLTYPE": "1",
                "ANI": "-",
                "DNIS": null,
                "EXTENSION": "-",
                "CALLINFO": null,
                "STARTTIME": "2018-04-03 07:03:49.0",
                "ENDTIME": "2018-04-03 07:06:24.0",
                "STIME": "155",
                "STAFF_ID": "-",
                "AGENT_ID": "-",
                "STAFFNAME": "-",
                "ROLEGROUP_ID": "-",
                "ROLEGROUPDESCRIPTION": "---",
                "CUSTOMER_GUID": "-",
                "CUSTOMER_ID": null,
                "CUSTOMERNAME": "-",
                "CUSTOMERLEVEL": "-",
                "IDTYPE": "0     ",
                "CUSTOMERID": null,
                "GENDER": null,
                "BIRTHDAY": null,
                "MOBILE": "-",
                "TEL_1": null,
                "TEL_2": null,
                "TEL_OTHERS": null,
                "ADDRESS": null,
                "WORKORDER_GUID": null,
                "WORKORDERID": null,
                "WORKORDERFRAME_ID": null,
                "WORKORDERSTATUS": null,
                "HF_TYPE": null,
                "HF_RESULT": null,
                "CREATEDBY": null,
                "CREATEDGROUP": null,
                "CREATEDDATE": null,
                "MODIFIEDDATE": null,
                "IN_DATE": "2018-04-04 15:29:21.0",
                "DIALECT_FLAG": null,
                "SATISFACTION": null,
                "VIP_FLAG": null,
                "POLICY_OBJECTIVE_GUID": null,
                "PRODUCT_NAME": null,
                "POLICY_CODE": null,
                "POLAPPLYAGE": null,
                "NEWPAYMODE": null,
                "WAY_NAME": null,
                "PAY_MODE": null,
                "SINGLE_MODE": null,
                "ORGAN_AREA": null,
                "SL_FLAG": null,
                "TOTAL_PREM": null,
                "CHARGE_YEAR": null,
                "AGENT_NAME": null,
                "PH_MOBILE": null
            }
        """

        self.full_data = """
        {
            "AREA_OF_JOB": "kf",
            "PROJECT_ID": "-    ",
            "ATTACHEDDATA": "-",
            "EVENT_GUID": "-",
            "RECORD_GUID": "-----------",
            "OBJECTIVETYPEID": "-",
            "OBJECTIVETYPE": "-",
            "OBJECTIVELEVEL": "-",
            "OBJECTIVE_GUID": "--",
            "CON": "1",
            "TYPEIDINFO": "-",
            "TYPEINFO": "--",
            "WORKINFO": null,
            "RECORDNO": "-",
            "DOCUMENTPATH": "ftp://-/2018_03_05/20180305090148100010900852.wav",
            "CALLTYPE": "2",
            "ANI": "-",
            "DNIS": "0-0",
            "EXTENSION": "-",
            "CALLINFO": "-",
            "STARTTIME": "2018-03-05 09:01:48.0",
            "ENDTIME": "2018-03-05 09:04:35.0",
            "STIME": "-",
            "STAFF_ID": "-",
            "AGENT_ID": "-",
            "STAFFNAME": "-",
            "ROLEGROUP_ID": "-Z",
            "ROLEGROUPDESCRIPTION": "---",
            "CUSTOMER_GUID": "-----",
            "CUSTOMER_ID": "00--",
            "CUSTOMERNAME": "----",
            "CUSTOMERLEVEL": null,
            "IDTYPE": "0     ",
            "CUSTOMERID": "-----",
            "GENDER": "M",
            "BIRTHDAY": "1981-06-10 00:00:00.0",
            "MOBILE": "1-1",
            "TEL_1": null,
            "TEL_2": null,
            "TEL_OTHERS": null,
            "ADDRESS": "四-室",
            "WORKORDER_GUID": null,
            "WORKORDERID": null,
            "WORKORDERFRAME_ID": null,
            "WORKORDERSTATUS": null,
            "HF_TYPE": "---",
            "HF_RESULT": "---",
            "CREATEDBY": null,
            "CREATEDGROUP": null,
            "CREATEDDATE": null,
            "MODIFIEDDATE": null,
            "IN_DATE": "2018-03-06 10:24:27.0",
            "DIALECT_FLAG": null,
            "SATISFACTION": null,
            "VIP_FLAG": null,
            "POLICY_OBJECTIVE_GUID": "-",
            "PRODUCT_NAME": "-- && --",
            "POLICY_CODE": "1234 && 123",
            "POLAPPLYAGE": "36 && 36",
            "NEWPAYMODE": "--- && ---",
            "WAY_NAME": "-- && --",
            "PAY_MODE": "-- && --",
            "SINGLE_MODE": "--- && ---",
            "ORGAN_AREA": "-- && --",
            "SL_FLAG": "0 && 0",
            "TOTAL_PREM": "- && -",
            "CHARGE_YEAR": "10 && 10",
            "AGENT_NAME": "- && -",
            "PH_MOBILE": "=-= && =-="
        }
        """

    def test_validate_vindex(self):
        content = "{}"
        (corrects, incorrects, _) = Processor().validate_vindex(content)
        self.assertEqual((corrects, incorrects),
                         (json.dumps(None), content))

        content = '[{"foo": "bar"}]'
        (corrects, incorrects, _) = Processor().validate_vindex(content)
        self.assertEqual((corrects, incorrects),
                         (content, json.dumps(None)))

    def test_transform_vindex_correct(self):
        attrs = {
            "mapping_fields": self.mapping_fields,
            "mapping_data": json.loads(self.mapping_data.strip())
        }
        (corrects, incorrects, _) = Processor().transform_vindex(
            attrs=attrs)
        corrects = json.loads(corrects)
        incorrects = json.loads(incorrects)

        self.assertTrue(incorrects is None)
        self.assertEqual("1522710384000", corrects["end_time"])
        self.assertEqual("1522710229000", corrects["start_time"])
        self.assertEqual("呼入", corrects["calltype"])
        self.assertEqual("kf1-20180403.wav", corrects["id"])
        self.assertEqual("20180403.wav", corrects["filename"])

    def test_full_transform_vindex(self):
        data = """
        {
            "AGENT_ID": "9-7",
            "ENDTIME": "2018-03-06 00:32:08.0",
            "WORKORDERID": null,
            "DIALECT_FLAG": null,
            "AGENT_NAME": null,
            "STAFF_ID": "0-0",
            "EXTENSION": "1-6",
            "POLICY_OBJECTIVE_GUID": null,
            "IN_DATE": "2018-04-23 10:41:59.0",
            "POLAPPLYAGE": null,
            "OBJECTIVELEVEL": "99",
            "DNIS": "8-0",
            "STIME": "1-6",
            "OBJECTIVETYPE": "---",
            "OBJECTIVETYPEID": "H-1",
            "DOCUMENTPATH": "ftp://-/2018_03_06/20180306002941100086900007.wav",
            "BIRTHDAY": "1962-05-10 00:00:00.0",
            "RECORD_GUID": "4-9",
            "IDTYPE": "0     ",
            "SL_FLAG": null,
            "ATTACHEDDATA": "0-1",
            "CUSTOMER_ID": "C-1",
            "MODIFIEDDATE": null,
            "PRODUCT_NAME": null,
            "ROLEGROUPDESCRIPTION": "---",
            "TEL_1": null,
            "TEL_2": null,
            "STAFFNAME": "---",
            "STARTTIME": "2018-03-06 00:29:42.0",
            "TYPEINFO": "---",
            "ADDRESS": "---",
            "WORKORDER_GUID": null,
            "ANI": "1-7",
            "CUSTOMERNAME": "---",
            "TYPEIDINFO": "H-1",
            "WAY_NAME": null,
            "TEL_OTHERS": null,
            "VIP_FLAG": null,
            "HF_TYPE": null,
            "PH_MOBILE": null,
            "RECORDNO": "1-0",
            "CREATEDDATE": null,
            "CUSTOMERID": "4-7",
            "GENDER": "F",
            "AREA_OF_JOB": "kf",
            "HF_RESULT": null,
            "CREATEDBY": null,
            "PROJECT_ID": "--",
            "SINGLE_MODE": null,
            "OBJECTIVE_GUID": null,
            "CON": "1",
            "CREATEDGROUP": null,
            "CHARGE_YEAR": null,
            "CALLINFO": "---",
            "ORGAN_AREA": null,
            "NEWPAYMODE": null,
            "PAY_MODE": null,
            "WORKORDERFRAME_ID": null,
            "CUSTOMERLEVEL": "A-1",
            "CALLTYPE": "1",
            "WORKORDERSTATUS": null,
            "SATISFACTION": null,
            "POLICY_CODE": null,
            "TOTAL_PREM": null,
            "WORKINFO": null,
            "CUSTOMER_GUID": "3-A",
            "EVENT_GUID": "C-F",
            "ROLEGROUP_ID": "T-7",
            "MOBILE": "1-7"
        }
        """
        attrs = {
            "mapping_data": json.loads(data.strip()),
            "mapping_fields": Config.HD_MAPPING_FEILDS,
            "fname_field": Config.HD_FILENAME_FIELD,
            "sttr_dirs": Config.HD_STTR_DIRS,
        }

        (corrects, incorrects, _) = Processor().transform_vindex(
            attrs=attrs)
        corrects = json.loads(corrects)
        incorrects = json.loads(incorrects)
        errors = incorrects["errors"].split(Config.ERROR_SEPARATOR)
        error_message = errors[0].split(Config.ERROR_MESSAGE_SEPARATOR)

        self.assertTrue(corrects is None)
        self.assertEqual(-12, int(error_message[-1]))

    def test_transform_vindex_incorrect(self):
        mapping_data = json.loads(copy.copy(self.mapping_data.strip()))
        del mapping_data["PH_MOBILE"]
        attrs = {
            "mapping_fields": self.mapping_fields,
            "mapping_data": mapping_data
        }
        (corrects, incorrects, _) = Processor().transform_vindex(
            attrs=attrs)
        corrects = json.loads(corrects)
        incorrects = json.loads(incorrects)
        errors = incorrects["errors"].split(Config.ERROR_SEPARATOR)
        error_message = errors[0].split(Config.ERROR_MESSAGE_SEPARATOR)

        self.assertTrue(corrects is None)
        self.assertTrue("errors" in incorrects)
        self.assertEqual(-3, int(error_message[-1]))
        self.assertEqual("1522710384000", incorrects["end_time"])
        self.assertEqual("1522710229000", incorrects["start_time"])
        self.assertEqual("呼入", incorrects["calltype"])
        self.assertEqual("kf1-20180403.wav", incorrects["id"])
        self.assertEqual("20180403.wav", incorrects["filename"])

    def test_transfer_vindex_sttr(self):
        self.base_dir = "./tests/resources"
        sttr_dirs = {
            "speed": os.path.join(self.base_dir, "speed"),
            "interrupt": os.path.join(self.base_dir, "interrupt"),
            "blankinfo": os.path.join(self.base_dir, "blankinfo")
        }
        mapping_data = json.loads(copy.copy(self.mapping_data.strip()))
        mapping_data["DOCUMENTPATH"] = "20180403090753100035900740.wav"
        attrs = {
            "mapping_data": mapping_data,
            "fname_field": "DOCUMENTPATH",
            "sttr_dirs": sttr_dirs
        }

        (corrects, incorrects, _) = Processor().transform_vindex(
            attrs=attrs)
        corrects = json.loads(corrects)
        incorrects = json.loads(incorrects)

        self.assertTrue(corrects is not None)
        self.assertTrue(incorrects is None)

    def test_transform_policy_index(self):
        str_data = """
        {
            "OBJECTIVE_GUID": "6-H",
            "ORGAN_ID": "8-1",
            "CUSTLISTNAME": "C-5",
            "ORGAN_NAME": "---",
            "ORGAN_AREA": "-",
            "SINGLE_CODE": "06",
            "SINGLE_MODE": "-",
            "WAY_CODE": "26",
            "WAY_NAME": "-",
            "INTERNAL_ID": "H-3",
            "PRODUCT_NAME": "-",
            "POLICY_CODE": "6-8",
            "PAY_MODE": "-",
            "CHARGE_DESC": "-",
            "CHARGE_YEAR": "20",
            "COVERAGE_DESC": "-",
            "COVERAGE_YEAR": "0",
            "PH_ID": "0-7",
            "PH_NAME": "-",
            "PH_CERTI_CODE": "5-X",
            "INSURED_NAME": "-",
            "PH_MOBILE": "-",
            "AGENT_CODE": "-",
            "AGENT_NAME": "-",
            "AGENT_MOBILE": "-",
            "TOTAL_PREM": "-",
            "ADDRESS": "---",
            "DEPT_NAME": "---",
            "SALE_NAME": "---",
            "WAY_BANK_NAME": "-",
            "BANK_NET": "-",
            "APPFLAG": "-  ",
            "ACCEPT_DATE": "2018-02-01 00:00:00.0",
            "END_DATE": null,
            "RECEIVED_DATE": "2018-02-28 00:00:00.0",
            "HF_DATE": "2018-03-05 00:00:00.0",
            "ACK_CUSTOMER_DATE": "2018-03-04 00:00:00.0",
            "HESITATEPERIOD_END_DATE": "2018-03-11 00:00:00.0",
            "IN_DATE": "2018-03-05 03:00:12.0",
            "LOGINNAME": "-",
            "STAFFNAME": "-",
            "HF_CALLEND_DATE": "2018-03-05 09:02:41.0",
            "HF_LASTCALL_DATE": "2018-03-05 12:12:29.0",
            "HFEND_MODIFY_DATE": "2018-03-05 12:12:29.0",
            "HESITATEPERIOD_YORN": "-",
            "HESITATEPERIOD_OVER": "2018-03-05 12:09:09.0",
            "CHECK_FLAG": "0",
            "REC_FLAG": "4",
            "IN_FLAG": "1",
            "F_FLAG": "0",
            "HF_CON": "1",
            "YORN_FLAG": "0",
            "SL_FLAG": "0",
            "RISK_FLAG": "0",
            "POLAPPLYDATE": "2018-02-01 00:00:00.0",
            "POLAPPLYAGE": "42",
            "RISKCODE": null,
            "RISKNAME": null,
            "SUMPREM": "-",
            "SUMPREM2": null,
            "PAY_MODE2": null,
            "CHARGE_DESC2": null,
            "CHARGE_YEAR2": null,
            "COVERAGE_DESC2": null,
            "COVERAGE_YEAR2": null,
            "W_FLAG": "0",
            "SLW_FLAG": null,
            "HF_TYPE": "-",
            "HF_RESULT": "-",
            "HJTP1": null,
            "HJTP1_Z": null,
            "HJTP1_Z1": null,
            "SECHFTYPE": null,
            "HF_SECTIME": null,
            "HJTP2": null,
            "HJTP2_Z": null,
            "HJTP2_Z1": null,
            "TRYTIMES": "4",
            "WORKORDERID": null,
            "WORKORDERSTATUSDESC": null,
            "CREATEDDATE": null,
            "WORKENDDATE": null,
            "MODIFIEDDATE_END": null,
            "CALLALLCON": "4",
            "CALLCON": "4",
            "CALLCON_S": "1",
            "CALL_FTIME": "2018-03-05 09:01:45.0",
            "CALL_FTEL": "-",
            "CALL_ETIME": "2018-03-05 12:09:09.0",
            "CALL_ETEL": "-",
            "CALL_EREC": "-",
            "WCON": "0",
            "WCON_W": "0",
            "WCON_WINFO": null,
            "WCON_Z": "0",
            "WCON_ZINFO": null,
            "OPEN_W": "0",
            "OPEN_WINFO": null,
            "OPEN_Z": "0",
            "OPEN_ZINFO": null,
            "WCALLCON": "0",
            "WCALLCON_S": "0",
            "WCALLCON_FTIME": null,
            "WCALLCON_FTEL": null,
            "WCALLCON_ETIME": null,
            "WCALLCON_ETEL": null,
            "WCALLCON_EREC": null,
            "HF_FLAG": "0",
            "HF_EH_RESULT": null,
            "NEWPAYMODE": "-",
            "SMS_FLAG": "1"
        }
        """
        data = str_data.strip()
        (corrects, _, _) = Processor().transform_policy_index(data)
        corrects = json.loads(corrects)

        data = json.loads(data)
        self.assertEqual(len(data) + 1, len(corrects))  # 多了一个id
        self.assertEqual(data["POLICY_CODE"], corrects["id"])

    def test_split_multiple_fields(self):
        data = json.loads(self.full_data)
        _index = HDIndex().mapping(
            mapping_fields=self.mapping_fields,
            mapping_data=data,
            case_sensitive=True)
        (corrects, _, _) = Processor().split_multiple_fields(_index._print())
        corrects = json.loads(corrects)[0]

        self.assertEqual(
            data["POLICY_CODE"].split(" && "), corrects["data_status_3"])

    def test_create_partition(self):
        _index = HDIndex().mapping(
            mapping_fields=self.mapping_fields,
            mapping_data=json.loads(self.full_data),
            case_sensitive=True)
        data = _index._print()
        (corrects, _, attributes) = Processor(
            is_test_mode=True).create_partition(data)
        corrects = json.loads(corrects)

        self.assertEqual(json.loads(data), corrects[0])
        self.assertEqual("20180305", attributes["batch"])
        self.assertEqual("2018-03-05", attributes["mysql_put_date"])
        self.assertEqual("kf", attributes["area_of_job"])
        self.assertEqual("20180305", attributes["date"])
        self.assertEqual("201803-0", attributes["recorddate"])
        self.assertEqual("20180305_hive", attributes["filename"])
        self.assertEqual(
            attributes["command"],
            Config.CREATE_HIVE_PARTITIONS_COMMAND_PATTERN.format(
                recorddate=attributes["recorddate"]))

    def test_speech_recognition(self):
        _attrs = {"dest_dir": "/test_file_dir"}
        (_, _, attributes) = Processor(
            is_test_mode=True).speech_recognition(None, _attrs)

        self.assertTrue(attributes is not None)

    def test_wav2png_data_none(self):
        _attrs = {
            "dest_dir": os.path.join(Config.BASE_DIR, "tests", "resources")
        }
        content = '[]'
        (corrects, incorrects, attributes) = Processor(
            is_test_mode=True).wav2png(content, _attrs)
        corrects = json.loads(corrects)

        self.assertEqual(corrects, [])
        self.assertEqual(attributes["command"], None)

    def test_wav2png(self):
        _attrs = {
            "dest_dir": os.path.join(Config.BASE_DIR, "tests", "resources")
        }
        content = [
            {"download_path": "/mnt/20180403090753100035900740.wav"}]
        content = json.dumps(content)
        (corrects, _, attributes) = Processor(
            is_test_mode=True).wav2png(content, _attrs)
        corrects = json.loads(corrects)
        content = json.loads(content)

        self.assertEqual(
            corrects[0]["id"], os.path.basename(content[0]["download_path"]))
        self.assertTrue(attributes["command"] is not None)

    def test_transform_btsm_vindex(self):
        content = """
        {
            "filename": "Y-277.wav",
            "area_of_job": "---",
            "start_time": "2018-03-25 21:00:51",
            "end_time": "2018-03-25 21:10:18",
            "duration": "567",
            "callnumber": "-",
            "rep_no": "-",
            "rep_group": "---",
            "agent_no": null,
            "account_no": null,
            "rep_team": null,
            "calltype": "---",
            "number_of_record": null,
            "region": null,
            "project_list": null,
            "satisfaction": null,
            "qa_no": null,
            "summarize_group": null,
            "summarize_class": null,
            "summarize_project": null,
            "summarize_content": null,
            "is_qa": null,
            "to_warehouse": null,
            "to_improvement": null,
            "isnormality": null,
            "type_of_service": null,
            "data_status": null,
            "data_status_2": null,
            "data_status_3": null,
            "group_leader": null,
            "cust_id": null,
            "cust_name": null,
            "audit_no": null,
            "phone_type": null,
            "verify_type": null,
            "staff_name": null,
            "bu_type": null,
            "rule_prompt": null,
            "id": null,
            "plaintextb": null,
            "speedresultb": null
        }
        """
        content = [json.loads(content.strip())]
        (_, incorrects, _) = Processor().transform_btsm_vindex(content)
        incorrects = json.loads(incorrects)

        ignore_fields = ["start_time", "end_time",
                         "id", "speedresultb", "plaintextb"]
        for field, value in incorrects[0].items():
            if field not in ignore_fields:
                self.assertEqual(value, incorrects[0][field])

    def test_incorrect_transform_btsm_vindex_speed_sttr_number_error(self):
        content = """
        {
            "filename" : "Y-277.wav",
            "area_of_job" : "---",
            "start_time" : "2018-03-30 20:28:19",
            "end_time" : "2018-03-30 20:34:50",
            "duration" : "391",
            "callnumber" : "-",
            "rep_no" : "-",
            "rep_group" : "---",
            "agent_no" : null,
            "account_no" : null,
            "rep_team" : null,
            "calltype" : "---",
            "number_of_record" : null,
            "region" : null,
            "project_list" : null,
            "satisfaction" : null,
            "qa_no" : null,
            "summarize_group" : null,
            "summarize_class" : null,
            "summarize_project" : null,
            "summarize_content" : null,
            "is_qa" : null,
            "to_warehouse" : null,
            "to_improvement" : null,
            "isnormality" : null,
            "type_of_service" : null,
            "data_status" : null,
            "data_status_2" : null,
            "data_status_3" : null,
            "group_leader" : null,
            "cust_id" : null,
            "cust_name" : null,
            "audit_no" : null,
            "phone_type" : null,
            "verify_type" : null,
            "staff_name" : null,
            "bu_type" : null,
            "rule_prompt" : null,
            "id" : null,
            "plaintextb" : null,
            "speedresultb" : null
        }
        """
        content = [json.loads(content.strip())]
        (_, incorrects, _) = Processor().transform_btsm_vindex(
            content)
        incorrects = json.loads(incorrects)
        errors = incorrects[0]["errors"].split(Config.ERROR_SEPARATOR)
        error_message = errors[0].split(Config.ERROR_MESSAGE_SEPARATOR)

        self.assertEqual(-7, int(error_message[-1]))

    def test_incorrect_transform_btsm_vindex(self):
        content = """
        {
            "filename": "Y-277ASDAS.wav",
            "area_of_job": "YX-IM",
            "start_time": "2018-03-25 21:00:51",
            "end_time": "2018-03-25 21:10:18",
            "duration": "567",
            "callnumber": "-",
            "rep_no": "-",
            "rep_group": "---",
            "agent_no": null,
            "account_no": null,
            "rep_team": null,
            "calltype": "---",
            "number_of_record": null,
            "region": null,
            "project_list": null,
            "satisfaction": null,
            "qa_no": null,
            "summarize_group": null,
            "summarize_class": null,
            "summarize_project": null,
            "summarize_content": null,
            "is_qa": null,
            "to_warehouse": null,
            "to_improvement": null,
            "isnormality": null,
            "type_of_service": null,
            "data_status": null,
            "data_status_2": null,
            "data_status_3": null,
            "group_leader": null,
            "cust_id": null,
            "cust_name": null,
            "audit_no": null,
            "phone_type": null,
            "verify_type": null,
            "staff_name": null,
            "bu_type": null,
            "rule_prompt": null,
            "id": null,
            "plaintextb": null,
            "speedresultb": null
        }
        """
        content = [json.loads(content.strip())]
        (_, incorrects, _) = Processor().transform_btsm_vindex(content)
        incorrects = json.loads(incorrects)
        errors = incorrects[0]["errors"].split(Config.ERROR_SEPARATOR)
        error_message = errors[0].split(Config.ERROR_MESSAGE_SEPARATOR)

        self.assertEqual(-14, int(error_message[-1]))

    def test_validate_btsm_sttr_file(self):
        fpath = os.path.join(Config.TESTS_DIR, "resources",
                             "BTSM", "20180424文本.txt")
        (is_error, errors) = Processor().validate_btsm_sttr_file(fpath)

        self.assertTrue(is_error)
        self.assertEqual(1, len(errors))

    def test_download_ftp_files(self):
        content = [{
            "STARTTIME": "2018-03-06 00:29:42.0",
            "DOCUMENTPATH": "ftp://-/2018_03_06/20180306002941100086900007.wav"
        }, {
            "STARTTIME": "2018-03-06 11:02:12.0",
            "DOCUMENTPATH": "ftp://-/2018_03_06/20180306110212100003900806.wav"
        }, {
            "STARTTIME": "2018-03-06 11:02:23.0",
            "DOCUMENTPATH": "ftp://-/2018_03_06/20180306110223100103900821.wav"
        }, {
            "STARTTIME": "2018-03-06 11:02:29.0",
            "DOCUMENTPATH": "ftp://-/2018_03_06/20180306110229100081900799.wav"
        }, {
            "STARTTIME": "2018-03-06 11:02:39.0",
            "DOCUMENTPATH": "ftp://-/2018_03_06/20180306110239100022900788.wav"
        }, {
            "STARTTIME": "2018-03-06 11:02:42.0",
            "DOCUMENTPATH": "ftp://-/2018_03_06/20180306110242100112907007.wav"
        }, {
            "STARTTIME": "2018-03-06 11:02:46.0",
            "DOCUMENTPATH": "ftp://-/2018_03_06/20180306110246100080900797.wav"
        }, {
            "STARTTIME": "2018-03-06 11:02:54.0",
            "DOCUMENTPATH": "ftp://-/2018_03_06/20180306110254100121900795.wav"
        }, {
            "STARTTIME": "2018-03-06 11:02:58.0",
            "DOCUMENTPATH": "ftp://-/2018_03_06/20180306110258100052900769.wav"
        }]

        content = json.dumps(content)
        (corrects, incorrects, attributes) = Processor(
            is_test_mode=True).download_ftp_files(content)

        self.assertTrue("ftp_download_root_dir" in attributes)
        self.assertTrue("dest_dir" in attributes)
        self.assertEqual(len(json.loads(content)), len(json.loads(incorrects)))


class TestUtils(unittest.TestCase):
    """ 测试工具类 """

    def setUp(self):
        self.base_dir = "./tests/resources"
        self.fname = "20180403090753100035900740.wav.txt"
        self.speed_fdir = os.path.join(self.base_dir, "speed")
        self.interrupt_fdir = os.path.join(self.base_dir, "interrupt")
        self.blankinfo_fdir = os.path.join(self.base_dir, "blankinfo")
        self.incorrect_fpath = os.path.join(self.base_dir,
                                            "filesize_error.file")

    def test_extract_filename(self):
        fname = Utils.extract_fname(__file__)
        self.assertEqual("tests", fname)

    def test_date_to_timestamp(self):
        _date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.assertEqual(13, len(Utils.date_to_timestamp(_date)))
        self.assertEqual(
            10, len(Utils.date_to_timestamp(_date, is_millisecond=False)))

    def test_date_to_ymd(self):
        _date = "2018-04-17 15:11:20"
        (year, month, day) = Utils.date_to_ymd(_date)
        self.assertEqual("2018", year)
        self.assertEqual("04", month)
        self.assertEqual("17", day)

    def test_timestamp_to_ymd(self):
        _date = "2018-04-17 15:11:20"
        timestamp = Utils.date_to_timestamp(_date)
        self.assertEqual(("2018", "04", "17"),
                         Utils.timestamp_to_ymd(timestamp))

    def test_timestamp_to_ymdhms(self):
        self.assertEqual(
            ("2018", "04", "27", "15", "39", "09"),
            Utils.timestamp_to_ymdhms("1524814749"))

    def test_timestamp_to_partiton(self):
        _date = "2018-04-01 15:11:20"
        timestamp = Utils.date_to_timestamp(_date)
        self.assertEqual("201804-0", Utils.timestamp_to_partiton(timestamp))

        _date = "2018-04-09 15:11:20"
        timestamp = Utils.date_to_timestamp(_date)
        self.assertEqual("201804-0", Utils.timestamp_to_partiton(timestamp))

        _date = "2018-04-10 15:11:20"
        timestamp = Utils.date_to_timestamp(_date)
        self.assertEqual("201804-1", Utils.timestamp_to_partiton(timestamp))

        _date = "2018-04-19 15:11:20"
        timestamp = Utils.date_to_timestamp(_date)
        self.assertEqual("201804-1", Utils.timestamp_to_partiton(timestamp))

        _date = "2018-04-20 15:11:20"
        timestamp = Utils.date_to_timestamp(_date)
        self.assertEqual("201804-2", Utils.timestamp_to_partiton(timestamp))

        _date = "2018-04-30 15:11:20"
        timestamp = Utils.date_to_timestamp(_date)
        self.assertEqual("201804-2", Utils.timestamp_to_partiton(timestamp))

        _date = "2018-05-31 15:11:20"
        timestamp = Utils.date_to_timestamp(_date)
        self.assertEqual("201805-2", Utils.timestamp_to_partiton(timestamp))

    def test_get_file_contents_correct(self):
        correct_filepath = os.path.join(self.speed_fdir, self.fname)
        contents = Utils.get_file_contents(correct_filepath)
        self.assertEqual(8, len(contents))

    def test_get_file_contents_incorrect(self):
        in_correct_filepath = "asdasfasdasd.wav"
        try:
            Utils.get_file_contents(in_correct_filepath)
        except Exception as e:
            self.assertEqual(-12, e.error_code)

    def test_filesize_error(self):
        in_correct_filepath = self.incorrect_fpath
        try:
            Utils.get_file_contents(in_correct_filepath)
        except Exception as e:
            self.assertEqual(-11, e.error_code)

    def test_extract_speed_stt_from_file(self):
        speep_sttr_filepath = os.path.join(self.speed_fdir, self.fname)
        speed_sttr = Utils.extract_speed_stt_from_file(
            None, speep_sttr_filepath)
        self.assertEqual(8, len(speed_sttr))

    def test_extract_interrupt_stt_from_file(self):
        interrupt_sttr_filepath = os.path.join(self.interrupt_fdir, self.fname)
        interrupt_sttr = Utils.extract_interrupt_stt_from_file(
            None, interrupt_sttr_filepath)
        self.assertEqual(8, len(interrupt_sttr))

    def test_extract_blankinfo_stt_from_file(self):
        blankinfo_sttr_filepath = os.path.join(self.blankinfo_fdir, self.fname)
        blankinfo_sttr = Utils.extract_blankinfo_stt_from_file(
            None, blankinfo_sttr_filepath)
        self.assertEqual(27, len(json.loads(blankinfo_sttr)))

    def test_extract_stt_from_file(self):
        fname = self.fname
        fdirs = {
            "speed": self.speed_fdir,
            "interrupt": self.interrupt_fdir,
            "blankinfo": self.blankinfo_fdir,
        }
        sttrs = Utils.extract_stt_from_file(fname, fdirs)
        self.assertEqual(8, len(sttrs["speed"]))
        self.assertEqual(8, len(sttrs["interrupt"]))
        self.assertEqual(27, len(json.loads(sttrs["blankinfo"])))

    def test_append_suffix_not_exists(self):
        self.assertEqual("/123/", Utils.append_suffix_not_exists("/123", "/"))

    def test_wav2png(self):
        input_dir = os.path.join(Config.BASE_DIR, "tests", "resources")
        output_dir = os.path.join(
            Config.BASE_DIR, "tests", "resources", "pngs")
        (cmd, _in, _out) = Utils.wav2png(
            input_dir, output_dir, is_test_mode=True)
        self.assertEqual(input_dir + "/", _in)
        self.assertEqual(output_dir + "/", _out)

    def test_get_thinkitfile_to_dict(self):
        fspeed = os.path.join(
            Config.TESTS_DIR, "resources", "BTSM", "20180424文本.txt")
        dict_fspeeds = Utils.get_thinkitfile_speed_to_dict(fspeed)

        self.assertEqual(1, len(dict_fspeeds))

    def test_get_dest_dir(self):
        self.assertEqual(
            Config.FTP_DOWNLOAD_ROOT_DIR + "/20180427/154758",
            Utils.get_dest_dir(now="1524815278"))

    def test_get_ftppath_dir(self):
        fpath = "ftp://10.0.3.24/1234/123123/123123/232/23/1524815278.wav"
        self.assertEqual("1234/123123/123123/232/23",
                         Utils.get_ftppath_dir(fpath))

        fpath = "ftp://10.0.3.24/1524815278.wav"
        self.assertEqual("", Utils.get_ftppath_dir(fpath))

    def test_get_download_fpath(self):
        fpath = "ftp://10.0.3.24/1234/123/1524815278.wav"
        (_src, _dest) = Utils.get_download_fpath(fpath)
        self.assertEqual(
            "ftp://{0}/1234/123/1524815278.wav".format(Config.FTP_HOST), _src)
        self.assertEqual(os.path.basename(fpath), os.path.basename(_dest))

    def test_get_download_fpath_customer_ftppath(self):
        fpath = "ftp://10.0.3.24/1234/123/1524815278.wav"
        ftppath = "customer/ftppath"
        (_src, _dest) = Utils.get_download_fpath(fpath, ftppath=ftppath)
        self.assertEqual(
            "ftp://{0}/{1}/1524815278.wav".format(
                Config.FTP_HOST, ftppath),
            _src)

    def test_get_download_fpath_customer_destdir(self):
        fpath = "ftp://10.0.3.24/1234/123/1524815278.wav"
        dest_dir = "/customer/dest/dir"
        (_src, _dest) = Utils.get_download_fpath(fpath, dest_dir=dest_dir)

        self.assertEqual("{0}/1524815278.wav".format(dest_dir), _dest)

    def test_isset_and_notnone(self):
        _dict = {"foo": "bar", "bar": None}

        self.assertTrue(Utils.isset_and_notnone("foo", _dict))
        self.assertTrue(not Utils.isset_and_notnone("foo1", _dict))
        self.assertTrue(not Utils.isset_and_notnone("bar", _dict))


class TestField(unittest.TestCase):
    """ 测试字段类 """

    def test_base_field(self):
        bf = BaseField()
        self.assertEqual(64, len(bf.fields))

    def test_hd_field(self):
        bf = HDField()
        self.assertEqual(97, len(bf.fields))


class TestIndex(unittest.TestCase):
    """ 测试索引类 """

    def setUp(self):
        self._mappings = {"_filename": "filename"}
        self.correct_data = {"_filename": "123"}
        self.incorrect_data = {"filename": "123"}
        self.hd_mappings = {
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
            "callinfo": "callinfo"
        }

        self.mapping_data = """
            {
                "AREA_OF_JOB": "kf",
                "PROJECT_ID": "-    ",
                "ATTACHEDDATA": "-",
                "EVENT_GUID": "-----------",
                "RECORD_GUID": "---------",
                "OBJECTIVETYPEID": "-",
                "OBJECTIVETYPE": "-",
                "OBJECTIVELEVEL": "-",
                "OBJECTIVE_GUID": null,
                "CON": "1",
                "TYPEIDINFO": "-",
                "TYPEINFO": "-",
                "WORKINFO": null,
                "RECORDNO": "-",
                "DOCUMENTPATH": "ftp://-/2018_04_03/20180403.wav",
                "CALLTYPE": "1",
                "ANI": "-",
                "DNIS": null,
                "EXTENSION": "-",
                "CALLINFO": null,
                "STARTTIME": "2018-04-03 07:03:49.0",
                "ENDTIME": "2018-04-03 07:06:24.0",
                "STIME": "-",
                "STAFF_ID": "-",
                "AGENT_ID": "-",
                "STAFFNAME": "-",
                "ROLEGROUP_ID": "-",
                "ROLEGROUPDESCRIPTION": "---",
                "CUSTOMER_GUID": "---------",
                "CUSTOMER_ID": null,
                "CUSTOMERNAME": "-",
                "CUSTOMERLEVEL": "-",
                "IDTYPE": "0     ",
                "CUSTOMERID": null,
                "GENDER": null,
                "BIRTHDAY": null,
                "MOBILE": "-",
                "TEL_1": null,
                "TEL_2": null,
                "TEL_OTHERS": null,
                "ADDRESS": null,
                "WORKORDER_GUID": null,
                "WORKORDERID": null,
                "WORKORDERFRAME_ID": null,
                "WORKORDERSTATUS": null,
                "HF_TYPE": null,
                "HF_RESULT": null,
                "CREATEDBY": null,
                "CREATEDGROUP": null,
                "CREATEDDATE": null,
                "MODIFIEDDATE": null,
                "IN_DATE": "2018-04-04 15:29:21.0",
                "DIALECT_FLAG": null,
                "SATISFACTION": null,
                "VIP_FLAG": null,
                "POLICY_OBJECTIVE_GUID": null,
                "PRODUCT_NAME": null,
                "POLICY_CODE": null,
                "POLAPPLYAGE": null,
                "NEWPAYMODE": null,
                "WAY_NAME": null,
                "PAY_MODE": null,
                "SINGLE_MODE": null,
                "ORGAN_AREA": null,
                "SL_FLAG": null,
                "TOTAL_PREM": null,
                "CHARGE_YEAR": null,
                "AGENT_NAME": null,
                "PH_MOBILE": null
            }
        """

    def test_base_index(self):
        data = {"filename": "123", "other_field": "234"}

        index = BaseIndex(data)
        serialized_index = index.serialized()
        self.assertTrue(isinstance(serialized_index, basestring))

        _index = BaseIndex(serialized_index)
        self.assertEqual(data["filename"], _index._get("filename"))
        self.assertEqual(data["other_field"], _index._get("other_field"))

    def test_mapping(self):
        str_index = BaseIndex().mapping(
            self._mappings, self.correct_data)._print()
        self.assertEqual(
            json.loads(str_index)["filename"],
            self.correct_data["_filename"])

    def test_mapping_incorrects(self):
        str_index = BaseIndex().mapping(
            self._mappings, self.incorrect_data)._print()
        incorrects = json.loads(str_index)
        errors = incorrects["errors"].split(Config.ERROR_SEPARATOR)[0]
        error_message = errors.split(Config.ERROR_MESSAGE_SEPARATOR)

        self.assertEqual(-3, int(error_message[-1]))

    def test_mapping_not_serialized(self):
        _json = BaseIndex(is_serialized=False).mapping(
            self._mappings, self.correct_data)._print()
        self.assertEqual(_json["filename"],
                         self.correct_data["_filename"])

    def test_transfer(self):
        data = {"start_time": "2018-04-17 15:11:20",
                "end_time": "2018-04-18 15:11:20",
                "filename": "123.wav",
                "area_of_job": "kf"}
        _json = BaseIndex(data, is_serialized=False)._print()
        self.assertEqual("1523949080000", _json["start_time"])
        self.assertEqual("1524035480000", _json["end_time"])
        self.assertEqual("123.wav", _json["filename"])
        self.assertEqual("kf", _json["area_of_job"])
        self.assertEqual("kf-123.wav", _json["id"])
        self.assertEqual("2018", _json["years"])
        self.assertEqual("04", _json["months"])
        self.assertEqual("17", _json["days"])

    def test_hd_index_fields(self):
        self.assertEqual(97, len(HDIndex().fields))

    def test_hd_index(self):
        data = {"filename": "123", "other_field": "234"}
        _json = HDIndex(data, is_serialized=False)._print()
        self.assertEqual(data["filename"], _json["filename"])
        self.assertEqual(data["filename"], _json["documentpath"])
        self.assertEqual(data["other_field"], _json["other_field"])

    def test_hd_index_set_index_without_filename(self):
        data = {"documentpath": "/path/to/123.wav"}
        _json = HDIndex(data, is_serialized=False)._print()
        self.assertEqual("123.wav", _json["filename"])
        self.assertEqual(data["documentpath"], _json["documentpath"])

    def test_index_upper_fieldname(self):
        data = {"DOCUMENTPATH": "123"}
        _json = HDIndex(data, is_serialized=False)._print()
        self.assertTrue("DOCUMENTPATH" not in _json)
        self.assertTrue("documentpath" in _json)

    def test_index_unset(self):
        data = {"POLICY_OBJECTIVE_GUID": "123"}
        _json = HDIndex(data, is_serialized=False)._print()
        self.assertTrue("POLICY_OBJECTIVE_GUID" not in _json)
        self.assertTrue("POLICY_OBJECTIVE_GUID".lower() not in _json)

    def test_mapping_data_isnull(self):
        mapping_data = {}
        _json = HDIndex(is_serialized=False).mapping(
            self.hd_mappings, mapping_data)._print()
        for (field, value) in _json.items():
            if field not in ["errors"]:
                self.assertTrue(value in ["", "0", "0.0"])

    def test_hd_index_mapping(self):
        mapping_data = json.loads(self.mapping_data.strip())
        hd_index = HDIndex(is_serialized=False)
        _json = hd_index.mapping(
            self.hd_mappings, mapping_data, case_sensitive=True)._print()
        self.assertEqual(len(_json) + 2, len(hd_index.fields))

        for (field, value) in mapping_data.items():
            field = field.lower()
            if isinstance(value, basestring):
                value = value.strip()
            if field not in ["policy_objective_guid", "in_date",  # 已删除
                             "starttime", "endtime",  # 转成时间戳
                             "calltype"]:  # 转中文
                self.assertEqual(_json[self.hd_mappings[field]], value)

    def test_update_sttr(self):
        content = {
            "speed": ["1.44 7.48 - 坐席 [Neu] 0.65 0.00 -- -- 5.13",
                      "8.72 9.50 - 客户 [Neu] 0.51 0.00 -- -- 5.13",
                      "10.24 12.40 - 坐席 [Neu] 0.67 0.00 -- -- 8.33",
                      "14.23 16.74 - 客户 [Neu] 0.58 0.00 -- -- 5.18",
                      "17.07 19.59 - 坐席 [Neu] 0.71 0.00 -- -- 6.75",
                      "21.79 25.08 - 客户 [Neu] 0.55 0.00 -- -- 5.78",
                      "26.41 29.05 - 坐席 [Neu] 0.98 0.00 -- -- 7.95",
                      "31.00 35.60 - 坐席 [Neu] 0.69 0.00 -- -- 5.87",
                      "36.41 44.85 - 坐席 [Neu] 0.62 0.00 -- -- 5.69",
                      "45.20 49.58 - 坐席 [Neu] 0.93 0.00 -- -- 7.08",
                      "51.43 55.29 - 客户 [Neu] 0.75 0.00 -- -- 5.96",
                      "56.51 58.53 - 坐席 [Neu] 0.83 0.00 -- -- 7.92",
                      "61.13 65.41 - 客户 [Neu] 0.71 0.00 -- -- 6.31",
                      "64.15 64.48 - 坐席 [Neu] 0.90 0.00 -- -- 9.09",
                      "65.65 72.27 - 坐席 [Neu] 0.97 0.00 -- -- 4.68",
                      "74.29 77.30 - 客户 [Neu] 0.88 0.00 -- -- 5.98",
                      "78.41 81.05 - 坐席 [Neu] 0.83 0.43 78.41 81.05 5.30",
                      "82.29 84.95 - 客户 [Neu] 0.92 0.00 -- -- 4.89",
                      "85.63 86.09 - 坐席 [Neu] 0.64 0.38 85.63 86.09 6.52"],
            "blankinfo": '[{"startBlankPos":"19.6","blankLen":"2.2"},{"startBlankPos":"58.5","blankLen":"2.6"},{"startBlankPos":"72.3","blankLen":"2.0"},{"startBlankPos":"100.1","blankLen":"2.6"},{"startBlankPos":"115.9","blankLen":"2.1"},{"startBlankPos":"177.8","blankLen":"2.5"},{"startBlankPos":"196.1","blankLen":"2.5"},{"startBlankPos":"236.0","blankLen":"2.2"},{"startBlankPos":"243.8","blankLen":"2.9"},{"startBlankPos":"268.5","blankLen":"2.5"},{"startBlankPos":"285.4","blankLen":"4.5"},{"startBlankPos":"314.9","blankLen":"3.6"},{"startBlankPos":"327.7","blankLen":"2.4"},{"startBlankPos":"346.9","blankLen":"3.3"},{"startBlankPos":"511.8","blankLen":"2.7"},{"startBlankPos":"536.9","blankLen":"2.0"},{"startBlankPos":"552.5","blankLen":"2.4"},{"startBlankPos":"568.4","blankLen":"2.2"},{"startBlankPos":"611.1","blankLen":"3.1"},{"startBlankPos":"623.9","blankLen":"2.5"},{"startBlankPos":"829.5","blankLen":"3.4"},{"startBlankPos":"864.4","blankLen":"3.2"},{"startBlankPos":"949.5","blankLen":"2.9"},{"startBlankPos":"960.2","blankLen":"2.2"},{"startBlankPos":"970.6","blankLen":"2.1"},{"startBlankPos":"974.7","blankLen":"2.0"},{"startBlankPos":"991.8","blankLen":"2.6"}]',
            "interrupt": ["64.15 64.48 坐席 0.45",
                          "135.66 136.32 坐席 0.64",
                          "302.32 302.58 坐席 0.58",
                          "324.21 325.24 坐席 0.32",
                          "340.86 341.54 坐席 0.75",
                          "389.42 389.68 坐席 1.00",
                          "464.92 465.30 客户 0.79",
                          "528.36 529.19 坐席 0.69"],
        }
        _index = HDIndex(is_serialized=False).update_sttr(
            content, TencentSTTParser())._print()

        self.assertEqual(13, len(_index["plaintexta"].split(";")))
        self.assertEqual(8, len(_index["plaintextb"].split(";")))

    def test_hd_index_gen_field_exists(self):
        content = {
            "speed": [],
            "interrupt": [],
            "blankinfo": "",
        }
        _index = HDIndex(is_serialized=False).update_sttr(
            content, TencentSTTParser())._print()

        self.assertTrue("gena" in _index)
        self.assertTrue("genb" in _index)

    def test_hd_index_emotionvalue_field_not_exists(self):
        content = {
            "speed": [],
            "interrupt": [],
            "blankinfo": "",
        }
        _index = HDIndex(is_serialized=False).update_sttr(
            content, TencentSTTParser())._print()

        self.assertTrue("emotionvaluea" not in _index)
        self.assertTrue("emotionvalueb" not in _index)


class TestTencentSTTParser(unittest.TestCase):
    """ 测试腾讯引擎语音识别结果解析类 """

    def test_parse_null(self):
        content = {}
        sttr = TencentSTTParser().parse(content)
        self.assertEqual(sttr, content)

    def test_parse_exists_field(self):
        content = {
            "speed": [],
            "interrupt": [],
            "blankinfo": "",
        }
        sttr = TencentSTTParser().parse(content)
        self.assertEqual(sttr, {'tonea': '', 'toneb': '',
                                'plaintexta': '', 'plaintextb': '',
                                'speeda': '', 'speedb': '',
                                'maxstartblankpos': 0, 'emovalueb': '',
                                'emovaluea': '', 'maxblanklen': 0,
                                'blankinfo': '', 'emotiona': '',
                                'robspeedb': '', 'robspeeda': '',
                                'emotionb': ''})

    def test_parse(self):
        content = {
            "speed": ["1.44 7.48 - 坐席 [Neu] 0.65 0.00 -- -- 5.13",
                      "8.72 9.50 - 客户 [Neu] 0.51 0.00 -- -- 5.13",
                      "10.24 12.40 - 坐席 [Neu] 0.67 0.00 -- -- 8.33",
                      "14.23 16.74 - 客户 [Neu] 0.58 0.00 -- -- 5.18",
                      "17.07 19.59 - 坐席 [Neu] 0.71 0.00 -- -- 6.75",
                      "21.79 25.08 - 客户 [Neu] 0.55 0.00 -- -- 5.78",
                      "26.41 29.05 - 坐席 [Neu] 0.98 0.00 -- -- 7.95",
                      "31.00 35.60 - 坐席 [Neu] 0.69 0.00 -- -- 5.87",
                      "36.41 44.85 - 坐席 [Neu] 0.62 0.00 -- -- 5.69",
                      "45.20 49.58 - 坐席 [Neu] 0.93 0.00 -- -- 7.08",
                      "51.43 55.29 - 客户 [Neu] 0.75 0.00 -- -- 5.96",
                      "56.51 58.53 - 坐席 [Neu] 0.83 0.00 -- -- 7.92",
                      "61.13 65.41 - 客户 [Neu] 0.71 0.00 -- -- 6.31",
                      "64.15 64.48 - 坐席 [Neu] 0.90 0.00 -- -- 9.09",
                      "65.65 72.27 - 坐席 [Neu] 0.97 0.00 -- -- 4.68",
                      "74.29 77.30 - 客户 [Neu] 0.88 0.00 -- -- 5.98",
                      "78.41 81.05 - 坐席 [Neu] 0.83 0.43 78.41 81.05 5.30",
                      "82.29 84.95 - 客户 [Neu] 0.92 0.00 -- -- 4.89",
                      "85.63 86.09 - 坐席 [Neu] 0.64 0.38 85.63 86.09 6.52"],
            "blankinfo": '[{"startBlankPos":"19.6","blankLen":"2.2"},{"startBlankPos":"58.5","blankLen":"2.6"}]',
            "interrupt": ["64.15 64.48 坐席 0.45",
                          "135.66 136.32 坐席 0.64",
                          "302.32 302.58 坐席 0.58",
                          "324.21 325.24 坐席 0.32",
                          "340.86 341.54 坐席 0.75",
                          "389.42 389.68 坐席 1.00",
                          "464.92 465.30 客户 0.79",
                          "528.36 529.19 坐席 0.69"],
        }
        sttr = TencentSTTParser().parse(content)

        self.assertEqual(13, len(sttr["plaintexta"].split(";")))
        self.assertEqual(8, len(sttr["plaintextb"].split(";")))
        self.assertEqual(15, len(sttr))


class TestThinkitSTTParser(unittest.TestCase):
    """ 测试中科信利引擎语音识别结果解析类 """

    def setUp(self):
        self.fspeed = os.path.join(
            Config.TESTS_DIR, "resources", "BTSM", "fragments.txt")
        self.separator = ";"

    def test_parse(self):
        content = {"speed": Utils.get_file_contents(self.fspeed)}
        sttr = ThinkitSTTParser().parse(content)
        self.assertEqual(len(sttr["speedresulta"].split(self.separator)),
                         len(sttr["plaintexta"].split(self.separator)))
        self.assertEqual(len(sttr["speedresultb"].split(self.separator)),
                         len(sttr["plaintextb"].split(self.separator)))


class TestBTSMIndex(unittest.TestCase):

    def setUp(self):
        self.findex = """
        {
            "filename": "277.wav",
            "area_of_job": "-",
            "start_time": "2018-03-25 21:00:51",
            "end_time": "2018-03-25 21:10:18",
            "duration": "567",
            "callnumber": "-",
            "rep_no": "-",
            "rep_group": "---",
            "agent_no": null,
            "account_no": null,
            "rep_team": null,
            "calltype": "---",
            "number_of_record": null,
            "region": null,
            "project_list": null,
            "satisfaction": null,
            "qa_no": null,
            "summarize_group": null,
            "summarize_class": null,
            "summarize_project": null,
            "summarize_content": null,
            "is_qa": null,
            "to_warehouse": null,
            "to_improvement": null,
            "isnormality": null,
            "type_of_service": null,
            "data_status": null,
            "data_status_2": null,
            "data_status_3": null,
            "group_leader": null,
            "cust_id": null,
            "cust_name": null,
            "audit_no": null,
            "phone_type": null,
            "verify_type": null,
            "staff_name": null,
            "bu_type": null,
            "rule_prompt": null,
            "id": null,
            "plaintextb": null,
            "speedresultb": null
        }
        """

    def test_index(self):
        findex = json.loads(self.findex.strip())
        _index = BaseIndex(data=findex, is_serialized=False)._print()
        for field, value in findex.items():
            if field not in ["start_time", "end_time", "id"]:
                self.assertEqual(value, _index[field])


if __name__ == "__main__":
    unittest.main()
