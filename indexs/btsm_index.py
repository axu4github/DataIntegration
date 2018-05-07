# coding=utf-8

from indexs.base_index import BaseIndex
from fields.base_field import BaseField


class BTSMIndex(BaseIndex):

    def __init__(self, data=None, field=BaseField(), is_serialized=True):
        super(BTSMIndex, self).__init__(
            data=data, field=field, is_serialized=is_serialized)

    def _transfer(self):
        super(BTSMIndex, self)._transfer()
        self._set("handledfilename", "b")
        return self
