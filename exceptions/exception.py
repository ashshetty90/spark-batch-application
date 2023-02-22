
class DataExtractionError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__("Failed to read files \r\n {}".format(self.message))


class DataTransformationError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__("Failed to transform files \r\n {}".format(self.message))


class DataLoadError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__("Failed to load files \r\n {}".format(self.message))


