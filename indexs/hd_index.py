# coding=utf-8

from indexs.base_index import BaseIndex
from fields.hd_field import HDField
import os


class HDIndex(BaseIndex):

    default_dateformat = "%Y-%m-%d %H:%M:%S.%f"
    stt_fields = ["plaintexta", "plaintextb",
                  "speedresulta", "speedresultb",
                  "tonea", "toneb",
                  "blankinfo", "maxblanklen", "maxstartblankpos",
                  "robspeeda", "robspeedb",
                  "emotiona", "emotionb",
                  "emovaluea", "emovalueb",
                  "gena", "genb"]

    def __init__(self, data=None, field=HDField(), is_serialized=True):
        super(HDIndex, self).__init__(
            data=data, field=field, is_serialized=is_serialized)

    def _transfer(self):
        if self._isset_and_isdefault("filename") and \
           self._isset_and_notdefault("documentpath"):
            self._set("filename", os.path.basename(self._get("documentpath")))

        super(HDIndex, self)._transfer()
        if self._isset_and_notdefault("calltype"):
            int_calltype = int(self._get("calltype"))
            if 1 == int_calltype:
                self._set("calltype", "呼入")
                if self._isset_and_notdefault("summarize_project"):
                    self._set("callnumber", self._get("summarize_project"))
            elif 2 == int_calltype:
                self._set("calltype", "呼出")
                if self._isset_and_notdefault("summarize_class"):
                    self._set("callnumber", self._get("summarize_class"))

        if self._isset_and_isdefault("documentpath"):
            self._set("documentpath", self._get("filename"))

        if self._isset("policy_objective_guid"):
            self._unset("policy_objective_guid")

        self._set("duration", int(round(float(self._get("duration")))))

        self._unset("emotionvaluea")
        self._unset("emotionvalueb")

        return self

    def update_sttr(self, content, stt_parser):
        super(HDIndex, self).update_sttr(content, stt_parser)
        for gen in ["gena", "genb"]:
            self._set(gen, "")

        return self
