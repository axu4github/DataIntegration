# coding=utf-8

from utils import Utils
from loggings import LoggableMixin
from indexs.hd_index import HDIndex
from indexs.btsm_index import BTSMIndex
from indexs.hd_policy_index import HDPolicyIndex
from stt_parsers.tencent_stt_parser import TencentSTTParser
from stt_parsers.thinkit_stt_parser import ThinkitSTTParser
from errors import (
    BaseError,
    SpeedSTTRIsNotFoundError,
    FileNameFieldNotFoundValueError,
    IsTestModeError,
    ArgumentsNotFoundError,
    ArgumentsError
)
from confs.configs import Config
import os
import shutil
import time
import base64
import json
import copy
from datetime import datetime
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


class ProcessorResponse(object):

    def __init__(self, corrects=None, incorrects=None, attributes=None):
        self.corrects = json.dumps(corrects)
        self.incorrects = json.dumps(incorrects)
        self.attributes = attributes

    def _print(self):
        return (self.corrects, self.incorrects, self.attributes)


class Processor(LoggableMixin):

    def __init__(self, is_test_mode=False):
        super(Processor, self).__init__()
        self.cmd_sleep = 20
        self.is_test_mode = is_test_mode
        self.auto_create = True
        if self.is_test_mode:
            self.auto_create = False

    def split_flowfiles_for_stt(self, content=None, _attrs=None):
        """
        由于语音识别模块是集群，所以再进行语音识别模块前需要将数据预先分开，
        同时由于执行识别是以目录为入口，所以需要将分离的语音文件移动到不同的目录中。
        """
        if "dest_dir" not in _attrs and not Utils.isempty(_attrs["dest_dir"]):
            raise ArgumentsError("dest_dir Is Not Set In Attributes.")

        result = []
        if content is not None:
            content = json.loads(content.strip())
            if not isinstance(content, list):
                content = [content]

            split_group_number = Utils.use_if_set_else_default(
                "split_group_number", _attrs, Config.DEFAULT_SPLIT_NUMBER)

            dest_dir = _attrs["dest_dir"]
            content_chunks = Utils.groups(content, split_group_number)
            for i, content_chunk in enumerate(content_chunks):
                _attrs = copy.copy(_attrs)
                _dest_dir = os.path.join(dest_dir, "CHUNK-{0}".format(i + 1))
                if not os.path.isdir(_dest_dir):
                    os.makedirs(_dest_dir)

                _attrs["dest_dir"] = _dest_dir
                for _file in content_chunk:
                    filename = os.path.basename(_file["DOCUMENTPATH"])
                    _src = os.path.join(dest_dir, filename)
                    _dest = os.path.join(_dest_dir, filename)
                    self.logger.debug(
                        "Copy Src:[{0}] To Dest:[{1}]".format(_src, _dest))
                    if not self.is_test_mode:
                        shutil.copyfile(_src, _dest)

                result.append(ProcessorResponse(
                    corrects=content_chunk, attributes=_attrs)._print())

        return result

    def move_file_to_ftp(self, content=None, attrs=None):
        """
        恒大生产环境要求：需要将挂载到中转服务器的数据目录中数据拷贝到FTP目录中，
                       不能直接从挂载目录中传输数据。
        """
        data_soruce_base_dir = Config.DATA_SOURCE_DIR
        ftp_base_dir = Config.FTP_DIR
        if content is not None:
            content = json.loads(content.strip())
            if not isinstance(content, list):
                content = [content]

            for _file in content:
                date_dir, filename = Utils.get_ftppath_dir_and_filename(
                    _file["DOCUMENTPATH"])

                if Utils.isset_and_notnone("data_soruce_base_dir", attrs):
                    data_soruce_base_dir = attrs["data_soruce_base_dir"]

                if Utils.isset_and_notnone("ftp_base_dir", attrs):
                    ftp_base_dir = attrs["ftp_base_dir"]

                src = os.path.join(
                    data_soruce_base_dir, date_dir, filename)
                if not os.path.isfile(src):
                    self.logger.warn("""
                        Data Source Path File: [{0}] Not Exists.
                    """.format(src).strip())
                    continue

                dest_dir = os.path.join(ftp_base_dir, date_dir)
                if not os.path.isdir(dest_dir):
                    os.makedirs(dest_dir)

                dest = os.path.join(dest_dir, filename)
                self.logger.debug(
                    "Copy Src:[{0}] To Dest:[{1}]".format(src, dest))
                shutil.copyfile(src, dest)

        return ProcessorResponse(corrects=content)._print()

    def validate_vindex(self, content=None, attrs=None):
        corrects, incorrects = None, None
        # 为了校验之后的数据拆分，把所有输入的Json对象都转化为Json数组
        if content is not None:
            content = json.loads(content.strip())
            if not isinstance(content, list):
                content = [content]

        if not Utils.jsonobj_isempty(content):
            corrects = content
        else:
            incorrects = content

        return ProcessorResponse(corrects=corrects,
                                 incorrects=incorrects)._print()

    def transform_vindex(self, content=None, attrs=None):
        if content is not None:
            content = json.loads(content)

        _index = HDIndex(data=content, is_serialized=False)
        if Utils.isset_and_notnone("mapping_fields", attrs) and \
           Utils.isset_and_notnone("mapping_data", attrs):
            _index.mapping(attrs["mapping_fields"],
                           attrs["mapping_data"],
                           case_sensitive=True)

        if Utils.isset_and_notnone("fname_field", attrs) and \
           Utils.isset_and_notnone("sttr_dirs", attrs):
            try:
                if Utils.isset_and_notnone("mapping_data", attrs) and \
                   attrs["fname_field"] in attrs["mapping_data"]:
                    fname = attrs["mapping_data"][attrs["fname_field"]]
                elif not Utils.jsonobj_isempty(content) and \
                        attrs["fname_field"] in content:
                    fname = content[attrs["fname_field"]]
                else:
                    raise FileNameFieldNotFoundValueError()

                sttr = Utils.extract_stt_from_file(
                    os.path.basename("{0}.txt".format(fname)),
                    attrs["sttr_dirs"])

                if "sttr_errors" in sttr:
                    if len(sttr["sttr_errors"]) > 0:
                        for err in sttr["sttr_errors"]:
                            _index._set_error(err)

                    del sttr["sttr_errors"]

                _index.update_sttr(sttr, TencentSTTParser())
            except BaseError as e:
                _index._set_error(e)

        _json = _index._print()
        if _index.error_field in _json and len(_json[_index.error_field]) > 0:
            return ProcessorResponse(incorrects=_json)._print()
        else:
            return ProcessorResponse(corrects=_json)._print()

    def transform_vindex_arr(self, content=None, attrs=None):
        result = []
        if not Utils.jsonobj_isempty(attrs):
            if "mapping_datas" in attrs and attrs["mapping_datas"] is not None:
                mapping_datas = attrs["mapping_datas"]
                if not isinstance(mapping_datas, list):
                    mapping_datas = [mapping_datas]

                for mapping_data in mapping_datas:
                    attr = {
                        "mapping_data": mapping_data,
                        "mapping_fields": attrs["mapping_fields"],
                    }

                    if "fname_field" in attrs:
                        attr["fname_field"] = attrs["fname_field"]

                    if "sttr_dirs" in attrs:
                        attr["sttr_dirs"] = attrs["sttr_dirs"]

                    (corrects, incorrects, _) = self.transform_vindex(
                        attrs=attr)
                    corrects = json.loads(corrects)
                    if corrects is not None:
                        content = corrects
                    else:
                        content = json.loads(incorrects)

                    result.append(content)

        return ProcessorResponse(corrects=result)._print()

    def transform_btsm_vindex(self, content, _attrs=None):
        corrects, incorrects, fpath = [], [], ""
        if content is not None:
            content = json.loads(content)
            if not isinstance(content, list):
                content = [content]

        if not Utils.jsonobj_isempty(content):
            if "absolute.path" in _attrs and "filename" in _attrs:
                fpath = os.path.join(
                    _attrs["absolute.path"],
                    _attrs["filename"].replace("index", "sttr"))

            self.logger.info(
                "INDEX File Path: {0}".format(
                    os.path.join(_attrs["absolute.path"], _attrs["filename"])))
            self.logger.info("STTR File Path: {0}".format(fpath))

            sttrs = Utils.get_thinkitfile_speed_to_dict(fpath)
            for (i, findex) in list(enumerate(content)):
                self.logger.info("Processing {0} File.".format(i))
                _index = BTSMIndex(is_serialized=False, data=findex)
                fname = _index._get("filename").lower()
                if fname in sttrs:
                    sttr = sttrs[fname]
                    _index.update_sttr({"speed": sttr}, ThinkitSTTParser())
                else:
                    _index._set_error(SpeedSTTRIsNotFoundError())

                dict_vindex = _index._print()
                if "errors" in dict_vindex and len(dict_vindex["errors"]) > 0:
                    incorrects.append(dict_vindex)
                else:
                    corrects.append(dict_vindex)

        return ProcessorResponse(
            corrects=corrects,
            incorrects=incorrects)._print()

    def split_multiple_fields(self, content, attrs=None):
        if content is not None:
            content = json.loads(content)
            if not isinstance(content, list):
                content = [content]

        for item in content:
            for field in Config.MULTIPLE_FIELDS:
                if field in item and item[field] is not None:
                    if field in ["satisfaction"]:  # 客户年龄列
                        if item[field]:
                            item[field] = map(
                                int, list(set(item[field].split(" && "))))
                        else:
                            item[field] = [0]
                    else:
                        item[field] = list(set(item[field].split(" && ")))

        return ProcessorResponse(corrects=content)._print()

    def transform_policy_index(self, content, attrs=None):
        content = json.loads(content.strip())
        _index = HDPolicyIndex(data=content, is_serialized=False)
        _json = _index._print()
        if _index.error_field in _json and len(_json[_index.error_field]) > 0:
            return ProcessorResponse(incorrects=_json)._print()
        else:
            return ProcessorResponse(corrects=_json)._print()

    def create_partition(self, content, _attrs=None):
        attrs = {
            "recorddate": "",
            "filename": "",
            "mysql_put_date": "",
            "area_of_job": "",
            "date": "",
            "batch": "",
            "command": "",
        }

        if content is not None:
            content = json.loads(content)
            if not isinstance(content, list):
                content = [content]

        for item in content:
            if "start_time" in item and "area_of_job" in item:
                start_time = item["start_time"]
                (year, mon, day) = Utils.timestamp_to_ymd(start_time)
                attrs["recorddate"] = Utils.timestamp_to_partiton(start_time)
                attrs["date"] = "{0}{1}{2}".format(year, mon, day)
                attrs["filename"] = "{0}_hive".format(attrs["date"])
                attrs["mysql_put_date"] = "{0}-{1}-{2}".format(year, mon, day)
                attrs["area_of_job"] = item["area_of_job"]
                attrs["batch"] = attrs["date"]
                break
            else:
                raise ArgumentsError("start_time or area_of_job is Empty.")

        if "recorddate" in attrs and attrs["recorddate"] != "":
            cmd = Config.CREATE_HIVE_PARTITIONS_COMMAND_PATTERN.format(
                recorddate=attrs["recorddate"]).strip()
            attrs["command"] = cmd
            self.logger.info("Create Partition CMD: {0}".format(cmd))
            if not self.is_test_mode:
                os.system(cmd)
                time.sleep(self.cmd_sleep)

        for (k, v) in attrs.items():
            if v == "":
                raise ArgumentsError("create_partition attrs value is Empty.")

        return ProcessorResponse(corrects=content, attributes=attrs)._print()

    def download_ftp_files(self, content, _attrs=None):
        corrects, incorrects, attrs = [], [], {}
        stt_error_field, _ftp = "stt_errors", None
        stt_error_field_default = []

        if content is not None:
            content = json.loads(content)
            if not isinstance(content, list):
                content = [content]

        if not Utils.jsonobj_isempty(content):
            if not self.is_test_mode:
                _ftp = Utils.set_ftp_handle()

            dest_dir = Utils.get_dest_dir(
                now=time.time(), auto_create=self.auto_create)
            self.logger.debug("FTP DOWNLOAD DIR: [{0}]".format(dest_dir))

            attrs = {
                "ftp_download_root_dir": Config.FTP_DOWNLOAD_ROOT_DIR,
                "dest_dir": dest_dir
            }
            for _file in content:
                if "DOCUMENTPATH" in _file:
                    _file[stt_error_field] = stt_error_field_default
                    (_src, _dest) = Utils.get_download_fpath(
                        _file["DOCUMENTPATH"], dest_dir=dest_dir)
                    self.logger.info("Download File: {0} => {1}.".format(
                        _src, _dest))
                    if self.is_test_mode:
                        _is_succ, message = False, str(IsTestModeError())
                    else:
                        (_is_succ, message) = Utils.download_ftp_file(
                            _src, _dest, ftp_handle=_ftp)

                    if _is_succ:
                        _file["download_path"] = _dest
                    else:
                        self.logger.error("{0} {1}".format(_src, message))
                        _file[stt_error_field].append(message)
                else:
                    _file[stt_error_field].append(
                        str(ArgumentsNotFoundError()))

                if stt_error_field in _file and \
                   len(_file[stt_error_field]) > 0:
                    incorrects.append(_file)
                else:
                    corrects.append(_file)

        return ProcessorResponse(corrects=corrects,
                                 incorrects=incorrects,
                                 attributes=attrs)._print()

    def wav2png(self, content, _attrs=None):
        """
        语音文件生成波形图
        """
        cmd = None
        if content is not None:
            content = json.loads(content)
            if not isinstance(content, list):
                content = [content]

        if not Utils.jsonobj_isempty(content):
            if "dest_dir" not in _attrs and \
               not Utils.isempty(_attrs["dest_dir"]):
                raise ArgumentsError("dest_dir Is Not Set In Attributes.")

            ftp_dw_root = Utils.use_if_set_else_default(
                "ftp_download_root_dir", _attrs, Config.FTP_DOWNLOAD_ROOT_DIR)
            wavform_root = Utils.use_if_set_else_default(
                "wavform_root", _attrs, Config.WAVFORM_ROOT_DIR)

            input_dir = _attrs["dest_dir"]
            output_dir = os.path.join(
                wavform_root, *input_dir.split(ftp_dw_root)[-1].split("/"))

            (cmd, _, _output_dir) = Utils.wav2png(
                input_dir, output_dir, self.is_test_mode)
            self.logger.info("WAV Transform PNG Command: {0}".format(cmd))
            for _file in content:
                if "download_path" in _file:
                    fname = os.path.basename(_file["download_path"])
                    png_path = os.path.join(
                        _output_dir, "{0}.png".format(fname))
                    self.logger.info("Get PNG Result [{0}]".format(png_path))
                    if not self.is_test_mode:
                        try:
                            _content = Utils.get_file_strcontents(png_path)
                            _file["waveform"] = "{0}{1}".format(
                                Config.WAVFORM_PREFIX,
                                base64.b64encode(_content))
                        except Exception as e:
                            _file["wav2png_errors"] = str(e)

                    _file["id"] = fname

        return ProcessorResponse(
            corrects=content, attributes={"command": cmd})._print()

    def speech_recognition(self, content, _attrs=None):
        if content is not None:
            content = json.loads(content)
            if not isinstance(content, list):
                content = [content]

        if "dest_dir" not in _attrs and not Utils.isempty(_attrs["dest_dir"]):
            raise ArgumentsError("dest_dir Is Not Set In Attributes.")

        cmd = Config.SPEECH_RECOGNITION_CMD.format(_attrs["dest_dir"])
        if not self.is_test_mode:
            (_dir, backup_dir) = Utils.backup_if_exists(
                Config.SPEECH_RECOGNITION_RESULT_DIR,
                now=datetime.now())
            self.logger.debug(
                "Backup SPEECH_RECOGNITION_RESULT_DIR To {0}".format(
                    backup_dir))

            _dirs = [
                _dir,
                os.path.join(_dir, "voiceconflict"),
                os.path.join(_dir, "voiceresult"),
                os.path.join(_dir, "voicesence")
            ]
            for _d in _dirs:
                os.makedirs(_d, 0755)

            self.logger.info("Speech Recognition Command: {0}".format(cmd))
            os.system(cmd)
            time.sleep(self.cmd_sleep)

        return ProcessorResponse(
            corrects=content, attributes={"command": cmd})._print()

    def validate_btsm_sttr_file(self, fpath):
        errors = []
        dict_sttrs = Utils.get_thinkitfile_speed_to_dict(fpath)
        for (fname, speed_sttr) in dict_sttrs.items():
            sttr = ThinkitSTTParser().parse({"speed": speed_sttr})
            errors += sttr["stt_pares_errors"]

        return (len(errors) > 0, errors)


if __name__ == "__main__":
    fpath = ""
    (is_error, errors) = Processor().validate_btsm_sttr_file(fpath)
    print(is_error, len(errors))
