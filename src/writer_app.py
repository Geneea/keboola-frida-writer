# coding=utf-8
# Python 3

from keboola import docker

class Params:

    def __init__(self, config):
        self.config = config

    @staticmethod
    def init(data_dir=''):
        return Params(docker.Config(data_dir))


class WriterApp:

    def __init__(self, *, data_dir=''):
        self.params = Params.init(data_dir)

    def run(self):
        pass
