from abc import ABCMeta, abstractmethod


class SerializableMixin(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def loads(self, content):
        pass

    @abstractmethod
    def dumps(self, content):
        pass

    def serialized(self, content):
        return self.dumps(content)

    def deserialized(self, content):
        return self.loads(content)
