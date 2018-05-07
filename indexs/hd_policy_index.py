# coding=utf-8

from indexs.base_index import BaseIndex
from fields.hd_policy_field import HDPolicyField
from errors import PolicyIndexUniqFieldNotFoundError


class HDPolicyIndex(BaseIndex):

    default_dateformat = "%Y-%m-%d %H:%M:%S.%f"

    def __init__(self, data=None, field=HDPolicyField(), is_serialized=True):
        super(HDPolicyIndex, self).__init__(
            data=data, field=field, is_serialized=is_serialized)

    def _transfer(self):
        if self._isset("policy_code"):
            self._set("id", self._get("policy_code"))
        else:
            self._set_error(PolicyIndexUniqFieldNotFoundError("policy_code"))
