# coding=utf-8

from stt_parsers.base_stt_parser import BaseSTTParser
from errors import (
    UnknowRoleError,
    TypeIsNotListError,
    SpeedSTTRNumberError,
    InterruptSTTRNumberError,
    IsNotJsonStringError
)
from utils import Utils
import json


class TencentSTTParser(BaseSTTParser):
    """ 腾讯引擎语音识别结果解析 """

    speed_split_number = 10
    interrupt_split_number = 4

    def __init__(self):
        super(TencentSTTParser, self).__init__()

    def parse_speed(self, content):
        plaintexta, plaintextb, emotiona, emotionb = [], [], [], []
        emotionvaluea, emotionvalueb, tonea, toneb = [], [], [], []
        speeda, speedb = [], []
        if not isinstance(content, list):
            raise TypeIsNotListError()

        if len(content) > 0:
            for (i, line) in list(enumerate(content)):
                splited = line.split(self.content_separator)
                if len(splited) != self.speed_split_number:
                    raise SpeedSTTRNumberError(i + 1)

                (start, end, plaintext, role, emotion, emotion_value,
                 tone_avg, tone_start, tone_end, speed) = splited
                if unicode(role) == self.rolea:
                    emotiona.append(emotion)
                    emotionvaluea.append(emotion_value)
                    tonea.append("{0} {1} {2}".format(
                        tone_avg, tone_start, tone_end))
                    speeda.append("{0} {1} {2}".format(start, end, speed))
                    plaintexta.append(plaintext)
                elif unicode(role) == self.roleb:
                    emotionb.append(emotion)
                    emotionvalueb.append(emotion_value)
                    toneb.append("{0} {1} {2}".format(
                        tone_avg, tone_start, tone_end))
                    speedb.append("{0} {1} {2}".format(start, end, speed))
                    plaintextb.append(plaintext)
                else:
                    raise UnknowRoleError(role)

        return {
            "plaintexta": self._format(plaintexta),
            "plaintextb": self._format(plaintextb),
            "emotiona": self._format(emotiona),
            "emotionb": self._format(emotionb),
            "emovaluea": self._format(emotionvaluea),
            "emovalueb": self._format(emotionvalueb),
            "tonea": self._format(tonea),
            "toneb": self._format(toneb),
            "speedresulta": self._format(speeda),
            "speedresultb": self._format(speedb),
        }

    def parse_interrupt(self, content):
        robspeeda, robspeedb = [], []
        if not isinstance(content, list):
            raise TypeIsNotListError()

        if len(content) > 0:
            for (i, line) in list(enumerate(content)):
                splited = line.split(self.content_separator)
                if len(splited) != self.interrupt_split_number:
                    raise InterruptSTTRNumberError(i + 1)

                (start, end, role, value) = splited
                if unicode(role) == self.rolea:
                    robspeeda.append(
                        "{0} - {1} - {2}".format(start, end, value))
                elif unicode(role) == self.roleb:
                    robspeedb.append(
                        "{0} - {1} - {2}".format(start, end, value))
                else:
                    raise UnknowRoleError(role)

        return {
            "robspeeda": self._format(robspeeda),
            "robspeedb": self._format(robspeedb),
        }

    def parse_blankinfo(self, content):
        blankinfo_obj, max_blank_len, max_start_blankpos = {}, 0, 0
        if not Utils.isempty(content):
            try:
                blankinfo_obj = json.loads(content)
            except Exception:
                raise IsNotJsonStringError()

        if not Utils.jsonobj_isempty(blankinfo_obj):
            blank_len_arr = [item["blankLen"] for item in blankinfo_obj]
            max_pos = blank_len_arr.index(max(blank_len_arr))
            max_blank_len = blankinfo_obj[max_pos]["blankLen"]
            max_start_blankpos = blankinfo_obj[max_pos]["startBlankPos"]

        return {
            "blankinfo": content,
            "maxblanklen": max_blank_len,
            "maxstartblankpos": max_start_blankpos
        }
