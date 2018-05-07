from fields.base_field import BaseField


class HDField(BaseField):
    extended_fields = [
        "documentpath",
        "objectivetypeid",
        "objectivelevel",
        "staff_id",
        "rolegroup_id",
        "record_guid",
        "event_guid",
        "con",
        "typeidinfo",
        "typeinfo",
        "total_prem",
        "charge_year",
        "agent_name",
        "ph_mobile",
        "customer_guid",
        "idtype",
        "birthday",
        "mobile",
        "tel_1",
        "tel_2",
        "tel_others",
        "address",
        "workorder_guid",
        "workorderframe_id",
        "workorderstatus",
        "hf_type",
        "hf_result",
        "createdby",
        "createdgroup",
        "createddate",
        "modifieddate",
        "workinfo",
        "callinfo"
    ]

    def __init__(self):
        super(HDField, self).__init__()
