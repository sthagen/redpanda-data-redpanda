from json import JSONEncoder

class DucktapeJSONEncoder(JSONEncoder):
    def default(self, obj): ...
