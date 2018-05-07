# coding=utf-8

from stt_parsers.base_stt_parser import BaseSTTParser
from errors import (
    TypeIsNotListError,
    SpeedSTTRNumberError,
    UnknowRoleError
)


class ThinkitSTTParser(BaseSTTParser):
    """ 中科信利引擎语音识别结果解析 """

    speed_split_number = 5
    speed_separator = "="
    content_separator = "\t"

    def __init__(self):
        super(ThinkitSTTParser, self).__init__()

    def parse_speed(self, content):
        speeda, speedb = [], []
        plaintexta, plaintextb = [], []
        if not isinstance(content, list):
            raise TypeIsNotListError()

        for (i, line) in list(enumerate(content)):
            splited = line.strip().split(self.content_separator)
            if len(splited) != self.speed_split_number:
                raise SpeedSTTRNumberError(
                    "Line: {0}, Count: {1}".format(i + 1, len(splited)))

            (start, end, plaintext, role, speed) = splited
            speed = speed.split(self.speed_separator)[-1]
            if unicode(role) == self.rolea:
                speeda.append(
                    "{0} {1} {2}".format(start, end, speed))
                plaintexta.append(plaintext)
            elif unicode(role) == self.roleb:
                speedb.append(
                    "{0} {1} {2}".format(start, end, speed))
                plaintextb.append(plaintext)
            else:
                raise UnknowRoleError(role)

        return {
            "plaintexta": self._format(plaintexta),
            "plaintextb": self._format(plaintextb),
            "speedresulta": self._format(speeda),
            "speedresultb": self._format(speedb),
        }

    def parse_interrupt(self, content):
        pass

    def parse_blankinfo(self, content):
        pass
