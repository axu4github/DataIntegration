# coding=utf-8

from fields.base_field import BaseField
from serializers.json_serializer import JsonSerializer
from loggings import LoggableMixin
from utils import Utils
from errors import (
    BaseError,
    NotBaseErrorClassError,
    MappingFieldNotInInputDatasError,
    STTParserIsNotExtendBaseSTTParserError
)
from stt_parsers.base_stt_parser import BaseSTTParser
from confs.configs import Config


class BaseIndex(JsonSerializer, LoggableMixin):

    """
    data (json string/json obj): index data
    """

    error_field = "errors"
    default_value = ""
    default_integer_value = "0"
    default_float_value = "0.0"
    default_dateformat = "%Y-%m-%d %H:%M:%S"
    stt_fields = ["plaintexta", "plaintextb",
                  "speedresulta", "speedresultb",
                  "tonea", "toneb",
                  "blankinfo", "maxblanklen", "maxstartblankpos",
                  "robspeeda", "robspeedb",
                  "emotiona", "emotionb",
                  "emotionvaluea", "emotionvalueb"]

    def __init__(self, data=None, field=BaseField(), is_serialized=True):
        super(BaseIndex, self).__init__()
        self.fields = field.fields
        self.integer_feilds = field.integer_feilds
        self.float_feilds = field.float_feilds
        self._index = {}
        self._init(data)
        self.is_serialized = is_serialized

    def _init(self, data=None):
        self._init_default()
        if data is not None:
            if isinstance(data, basestring):
                data = self.deserialized(data)

            for (field, value) in data.items():
                self._set(field, value)

    def _init_default(self):
        for field in self.fields:
            self._set(field, self.default_value)

        if len(self.integer_feilds) > 0:
            for field in self.integer_feilds:
                self._set(field, self.default_integer_value)

        if len(self.float_feilds) > 0:
            for field in self.float_feilds:
                self._set(field, self.default_float_value)

    def _set(self, field, value):
        if isinstance(value, basestring):
            value = value.strip()

        self._index[field.lower()] = value

    def _get(self, field):
        value, field = None, field.lower()
        if self._isset(field):
            value = self._index[field]

        return value

    def _unset(self, field):
        field = field.lower()
        if self._isset(field):
            del self._index[field]

    def _isset(self, field):
        return field in self._index

    def _notset(self, field):
        return not self._isset(field)

    def _set_error(self, error):
        self.logger.error("{0}".format(str(error)))
        if not isinstance(error, BaseError):
            raise NotBaseErrorClassError()

        error_message = Config.ERROR_MESSAGE_SEPARATOR.join(
            [str(error), str(error.error_code)])
        if self._isset_and_isdefault(self.error_field):
            errors = [error_message]
        else:
            errors = self._get(self.error_field).split(
                Config.ERROR_SEPARATOR)
            errors.append(error_message)

        self._set(self.error_field, Config.ERROR_SEPARATOR.join(errors))

    def _isset_and_notdefault(self, field):
        return self._isset(field) and \
            self._get(field) != self.default_value

    def _isset_and_isdefault(self, field):
        return self._isset(field) and \
            self._get(field) == self.default_value

    def _transfer(self):
        if self._isset_and_notdefault("start_time"):
            (year, month, day) = Utils.date_to_ymd(
                self._get("start_time"), self.default_dateformat)
            self._set("years", year)
            self._set("months", month)
            self._set("days", day)

            start_time = Utils.date_to_timestamp(
                self._get("start_time"), self.default_dateformat)
            self._set("start_time", start_time)
            end_time = Utils.date_to_timestamp(
                self._get("end_time"), self.default_dateformat)
            self._set("end_time", end_time)

        if (self._isset_and_isdefault("id") or self._get("id") is None) and \
           self._isset_and_notdefault("area_of_job") and \
           self._isset_and_notdefault("filename"):
            self._set("id", "{0}-{1}".format(
                self._get("area_of_job").lower(),
                self._get("filename").lower()))

        return self

    def _print(self):
        _print_debug = []
        for (k, v) in self._index.items():
            _print_debug.append("{0} => {1}".format(k, v))

        self.logger.debug(", ".join(_print_debug))

        self._transfer()
        if self.is_serialized:
            return self.serialized()
        else:
            return self._index

    def serialized(self):
        return super(BaseIndex, self).serialized(self._index)

    def mapping(self, mapping_fields, mapping_data, case_sensitive=False):
        if mapping_data != {}:
            if case_sensitive:
                tmp = {}
                for (field, value) in mapping_data.items():
                    tmp[field.lower()] = value

                mapping_data = tmp

            for (mapping_field, field) in mapping_fields.items():
                try:
                    mapping_field = mapping_field.lower()
                    if mapping_field in mapping_data:
                        self._set(field, mapping_data[mapping_field])
                    else:
                        raise MappingFieldNotInInputDatasError(mapping_field)
                except BaseError as e:
                    self._set_error(e)
        return self

    def update_sttr(self, content, stt_parser):
        """ STTR: Speech To Text Result """
        try:
            if not isinstance(stt_parser, BaseSTTParser):
                raise STTParserIsNotExtendBaseSTTParserError()

            sttr = stt_parser.parse(content)
            for (field, value) in sttr.items():
                if field in self.stt_fields and not Utils.isempty(value):
                    self._set(field, value)
        except BaseError as e:
            self._set_error(e)

        return self

    def __str__(self):
        return self.serialized()
