from java.nio.charset import StandardCharsets
from org.apache.commons.io import IOUtils
from org.apache.nifi.processor.io import (
    InputStreamCallback, OutputStreamCallback
)
from processor import Processor
from confs.configs import Config
from utils import Utils
import json

###############################################################################
# Variables provided in scope by script engine:
#
#    session - ProcessSession
#    context - ProcessContext
#    log - ComponentLog
#    REL_SUCCESS - Relationship
#    REL_FAILURE - Relationship
###############################################################################

_session = session
_log = log
_succ = REL_SUCCESS
_fail = REL_FAILURE


class OutputStream(OutputStreamCallback):

    def __init__(self, content):
        self.content = str(content)

    def process(self, outputStream):
        outputStream.write(bytearray(self.content.encode("utf-8")))


class InputStream(InputStreamCallback):

    def __init__(self, flowfile, _session):
        self.flowfile = flowfile
        self._session = _session
        self._attrs = {}
        for (k, v) in self.flowfile.getAttributes().iteritems():
            self._attrs[k] = v

        self._attrs["mapping_fields"] = Config.HD_MAPPING_FEILDS
        self._attrs["fname_field"] = Config.HD_FILENAME_FIELD
        self._attrs["sttr_dirs"] = Config.HD_STTR_DIRS

    def process(self, inputStream):
        content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        self._attrs["mapping_data"] = json.loads(content.strip())

        (corrects, incorrects, attributes) = Processor().transform_vindex(
            attrs=self._attrs)
        self._transfer(corrects, attributes, _succ)
        self._transfer(incorrects, attributes, _fail)

    def _transfer(self, _content, _attributes, _relationship):
        if not Utils.isempty(_content):
            _flowfile = self._session.create(self.flowfile)
            _flowfile = self._session.write(_flowfile, OutputStream(_content))
            if _attributes is not None:
                _flowfile = self._session.putAllAttributes(
                    _flowfile, _attributes)
            self._session.transfer(_flowfile, _relationship)


flowfile = _session.get()
if(flowfile is not None):
    _session.read(flowfile, InputStream(flowfile, _session))
    _session.remove(flowfile)
