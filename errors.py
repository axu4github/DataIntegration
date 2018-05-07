class ArgumentsError(RuntimeError):

    def __init__(self):
        super(ArgumentsError, self).__init__()


class BaseError(RuntimeError):

    default_error_code = -99
    error_codes = {
        "InputEmptyError": -1,
        "NotBaseIndexClassError": -2,
        "MappingFieldNotInInputDatasError": -3,
        "NotBaseErrorClassError": -4,
        "UnknowRoleError": -5,
        "TypeIsNotListError": -6,
        "SpeedSTTRNumberError": -7,
        "InterruptSTTRNumberError": -8,
        "IsNotJsonStringError": -9,
        "STTParserIsNotExtendBaseSTTParserError": -10,
        "FileNotContentError": -11,
        "FileNotFoundError": -12,
        "PolicyIndexUniqFieldNotFoundError": -13,
        "SpeedSTTRIsNotFoundError": -14,
        "FileNameFieldNotFoundValueError": -15,
        "IsTestModeError": -16,
        "ArgumentsNotFoundError": -17
    }

    def __init__(self, error_message=None):
        self.classname = type(self).__name__
        self.error_code = self.default_error_code
        self.error_message = error_message
        if self.classname in self.error_codes:
            self.error_code = self.error_codes[self.classname]

    def __str__(self):
        if self.error_message is None:
            return "{0}({1}) ERROR".format(self.classname, self.error_code)
        else:
            return "{0} {1}({2}) ERROR".format(
                self.error_message, self.classname, self.error_code)


class InputEmptyError(BaseError):
    pass


class NotBaseIndexClassError(BaseError):
    pass


class MappingFieldNotInInputDatasError(BaseError):
    pass


class NotBaseErrorClassError(BaseError):
    pass


class UnknowRoleError(BaseError):
    pass


class TypeIsNotListError(BaseError):
    pass


class SpeedSTTRNumberError(BaseError):
    pass


class InterruptSTTRNumberError(BaseError):
    pass


class IsNotJsonStringError(BaseError):
    pass


class STTParserIsNotExtendBaseSTTParserError(BaseError):
    pass


class FileNotContentError(BaseError):
    pass


class FileNotFoundError(BaseError):
    pass


class PolicyIndexUniqFieldNotFoundError(BaseError):
    pass


class SpeedSTTRIsNotFoundError(BaseError):
    pass


class FileNameFieldNotFoundValueError(BaseError):
    pass


class IsTestModeError(BaseError):
    pass


class ArgumentsNotFoundError(BaseError):
    pass
