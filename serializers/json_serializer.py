from serialization import SerializableMixin
import json


class JsonSerializer(SerializableMixin):

    def loads(self, content):
        return json.loads(content)

    def dumps(self, content):
        return json.dumps(content)
