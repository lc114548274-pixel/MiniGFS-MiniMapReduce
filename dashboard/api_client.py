from minigfs.client import Client


class API(Client):
    def get(self, path: str):
        return self.request('GET', path)

    def post(self, path: str, data=None):
        return self.request('POST', path, **({'json': data} if data is not None else {}))

