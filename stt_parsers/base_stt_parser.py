# coding=utf-8

from abc import ABCMeta, abstractmethod


class BaseSTTParser(object):
    """ 语音识别结果解析基础类 """

    __metaclass__ = ABCMeta

    rolea = u"坐席"
    roleb = u"客户"
    content_separator = " "
    format_separator = ";"

    def __init__(self):
        super(BaseSTTParser, self).__init__()

    def _format(self, _list):
        if len(_list) > 0:
            return "{0}{1}".format(
                self.format_separator.join(_list), self.format_separator)
        else:
            return ""

    def parse(self, content):
        stt_pares_errors = []
        sttr = {}
        try:
            if "speed" in content:
                speed = self.parse_speed(content["speed"])
                sttr = dict(sttr, **speed)
        except Exception as e:
            stt_pares_errors.append(e)

        try:
            if "interrupt" in content:
                interrupt = self.parse_interrupt(content["interrupt"])
                sttr = dict(sttr, **interrupt)
        except Exception as e:
            stt_pares_errors.append(e)

        try:
            if "blankinfo" in content:
                blankinfo = self.parse_blankinfo(content["blankinfo"])
                sttr = dict(sttr, **blankinfo)
        except Exception as e:
            stt_pares_errors.append(e)

        sttr["stt_pares_errors"] = stt_pares_errors
        return sttr

    @abstractmethod
    def parse_speed(self, content):
        pass

    @abstractmethod
    def parse_interrupt(self, content):
        pass

    @abstractmethod
    def parse_blankinfo(self, content):
        pass
