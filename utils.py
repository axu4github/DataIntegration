# coding=utf-8

import json
import os
import time
from datetime import datetime
from errors import (
    BaseError,
    FileNotContentError,
    FileNotFoundError,
    ArgumentsError
)
from confs.configs import Config
from ftplib import FTP
import sys
reload(sys)
sys.setdefaultencoding('utf8')


class Utils(object):

    @staticmethod
    def isset_and_notnone(k, _dict):
        return k in _dict and _dict[k] is not None

    @staticmethod
    def isempty(content):
        if isinstance(content, int):
            return content == 0
        elif isinstance(content, basestring):
            return content.lower() in ["", "null", "[]"]
        else:
            raise ArgumentsError()

    @staticmethod
    def jsonstr_isempty(content):
        return json.loads(content) in [{}]

    @staticmethod
    def jsonobj_isempty(jsonobj):
        return jsonobj in [{}, [], [{}]]

    @staticmethod
    def extract_fname(filepath):
        (fname, fext) = os.path.splitext(os.path.basename(filepath))
        return fname

    @staticmethod
    def date_to_timestamp(date, dateformat=Config.DEFAULT_DATE_FORMAT,
                          is_millisecond=True):
        timestamp = str(int(time.mktime(time.strptime(date, dateformat))))
        if is_millisecond:
            timestamp = "{0}000".format(timestamp)
        return timestamp

    @staticmethod
    def date_to_ymd(date, dateformat=Config.DEFAULT_DATE_FORMAT):
        ymd = datetime.strptime(date, dateformat).strftime("%Y-%m-%d")
        return tuple(ymd.split("-"))

    @staticmethod
    def timestamp_to_ymd(timestamp):
        timestamp = int(timestamp)
        if len(str(timestamp)) == 13:
            timestamp = timestamp / 1000

        ymd = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
        return tuple(ymd.split("-"))

    @staticmethod
    def timestamp_to_ymdhms(timestamp):
        timestamp = int(timestamp)
        if len(str(timestamp)) == 13:
            timestamp = timestamp / 1000

        ymd = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d-%H-%M-%S")
        return tuple(ymd.split("-"))

    @staticmethod
    def timestamp_to_partiton(timestamp):
        (year, mon, day) = Utils.timestamp_to_ymd(timestamp)
        index = int(day) / 10
        if index > 2:
            index = "2"

        return "{0}{1}-{2}".format(year, mon, index)

    @staticmethod
    def extract_stt_from_file(fname, fdirs):
        sttr_errors = []
        try:
            speed_sttr = []
            if "speed" in fdirs:
                speed_sttr = Utils.extract_speed_stt_from_file(
                    fname, os.path.join(fdirs["speed"], fname))
        except BaseError as e:
            sttr_errors.append(e)

        try:
            interrupt_sttr = []
            if "interrupt" in fdirs:
                interrupt_sttr = Utils.extract_interrupt_stt_from_file(
                    fname, os.path.join(fdirs["interrupt"], fname))
        except BaseError as e:
            sttr_errors.append(e)

        try:
            blankinfo_sttr = ""
            if "blankinfo" in fdirs:
                blankinfo_sttr = Utils.extract_blankinfo_stt_from_file(
                    fname, os.path.join(fdirs["blankinfo"], fname))
        except BaseError as e:
            sttr_errors.append(e)

        return {
            "sttr_errors": sttr_errors,
            "speed": speed_sttr,
            "interrupt": interrupt_sttr,
            "blankinfo": blankinfo_sttr
        }

    @staticmethod
    def extract_speed_stt_from_file(fname, speep_sttr_filepath):
        return Utils.get_file_contents(speep_sttr_filepath)

    @staticmethod
    def extract_interrupt_stt_from_file(fname, interrupt_sttr_filepath):
        return Utils.get_file_contents(interrupt_sttr_filepath)

    @staticmethod
    def extract_blankinfo_stt_from_file(fname, blankinfo_sttr_filepath):
        return Utils.get_file_contents(blankinfo_sttr_filepath)[0]

    @staticmethod
    def get_file_contents(filepath):
        contents = []
        filepath = filepath.strip()
        if os.path.exists(filepath) and os.path.isfile(filepath):
            if os.path.getsize(filepath) > 3:
                with open(filepath, "r") as f:
                    contents = [line.strip() for line in f.readlines()]
            else:
                raise FileNotContentError(filepath)
        else:
            raise FileNotFoundError(filepath)

        return contents

    @staticmethod
    def get_file_strcontents(filepath):
        contents = None
        filepath = filepath.strip()
        if os.path.exists(filepath) and os.path.isfile(filepath):
            with open(filepath, "r") as f:
                contents = f.read()
        else:
            raise FileNotFoundError(filepath)

        return contents

    @staticmethod
    def append_suffix_not_exists(data, suffix="/"):
        if isinstance(data, basestring):
            if not data.endswith(suffix):
                data = "{0}{1}".format(data, suffix)

        return data

    @staticmethod
    def wav2png(input_dir=None, output_dir=None, is_test_mode=False):
        if input_dir is None or output_dir is None:
            raise ArgumentsError()

        input_dir = Utils.append_suffix_not_exists(input_dir, "/")
        output_dir = Utils.append_suffix_not_exists(output_dir, "/")

        if not os.path.exists(input_dir) or not os.path.isdir(input_dir):
            raise FileNotFoundError(input_dir)

        if not os.path.exists(output_dir) or not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        cmd = Config.WAV_TO_PNG_COMMAND.format(
            input_dir=input_dir, output_dir=output_dir)
        if not is_test_mode:
            os.system(cmd)

        return (cmd, input_dir, output_dir)

    @staticmethod
    def backup_if_exists(_dir, now=datetime.now()):
        backup_dir = None
        if os.path.exists(_dir) and os.path.isdir(_dir):
            backup_dir = "{0}_{1}".format(
                Utils.append_suffix_not_exists(_dir, "/")[:-1],
                now.strftime(Config.BACKUP_DIR_DATE_FORMAT))
            os.rename(_dir, backup_dir)

        return (_dir, backup_dir)

    @staticmethod
    def get_thinkitfile_speed_to_dict(fpath):
        fspeeds, fsuffix, content_separator = {}, ".wav", "\t"
        lines = Utils.get_file_contents(fpath)
        if len(lines) > 0:
            for line in lines:
                line = line.strip()
                if line.endswith(fsuffix):
                    current_file = line.lower()
                    fspeeds[current_file] = []
                elif len(line.split(content_separator)) > 1:
                    fspeeds[current_file].append(line)

        return fspeeds

    @staticmethod
    def set_ftp_handle():
        _ftp = FTP(Config.FTP_HOST)
        _ftp.login(Config.FTP_USER, Config.FTP_PASS)
        return _ftp

    @staticmethod
    def get_ftppath_dir(ftp_path):
        return "/".join(ftp_path.split("/")[3:-1])

    @staticmethod
    def get_ftppath_dir_and_filename(ftppath):
        return (Utils.get_ftppath_dir(ftppath), os.path.basename(ftppath))

    @staticmethod
    def get_download_fpath(fpath, ftppath=None, dest_dir=None):
        fname = os.path.basename(fpath)
        if ftppath is None:
            ftppath = Utils.get_ftppath_dir(fpath)

        if dest_dir is None:
            dest_dir = Utils.get_dest_dir()

        return (
            "ftp://{0}/{1}/{2}".format(Config.FTP_HOST, ftppath, fname),
            os.path.join(dest_dir, fname)
        )

    @staticmethod
    def get_dest_dir(now=time.time(),
                     ftp_dw_root=Config.FTP_DOWNLOAD_ROOT_DIR,
                     auto_create=True):
        dest_dir = "{0}/{1}{2}{3}/{4}{5}{6}".format(
            ftp_dw_root, *Utils.timestamp_to_ymdhms(now))

        if not os.path.isdir(dest_dir):
            if auto_create:
                os.makedirs(dest_dir)

        return dest_dir

    @staticmethod
    def download_ftp_file(_src, _dest, ftp_handle=None):
        _is_succ, message = False, ""
        try:
            if ftp_handle is None:
                ftp_handle = Utils.set_ftp_handle()

            ftp_handle.cwd("/{0}".format(Utils.get_ftppath_dir(_src)))
            ftp_handle.retrbinary("RETR %s" % os.path.basename(_src),
                                  open(_dest, "wb").write)
            ftp_handle.cwd("/")
            _is_succ = True
        except Exception as e:
            message = str(e)
            if os.path.exists(_dest) and os.path.isfile(_dest):
                os.remove(_dest)

        return (_is_succ, message)

    @staticmethod
    def groups(l, n):
        """ 共分多少组 """
        gn = len(l) / n
        _list = list(Utils.chunks(l, gn))
        _len = len(_list)
        if _len != n:
            _list[_len - 2] = _list[_len - 2] + _list[_len - 1]
            del(_list[_len - 1])

        return _list

    @staticmethod
    def chunks(l, n):
        """ 多少元素分一组 """
        for i in xrange(0, len(l), n):
            yield l[i:i + n]
