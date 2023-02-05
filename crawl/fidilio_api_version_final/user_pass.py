class DatabaseServerLoginInfo:

    def __init__(self):
        self.username = ""
        self.password = ""
        self.host = ""
        self.port = 0


class DatabaseLocalLoginInfo:

    def __init__(self):
        self.username = "root"
        self.password = "12345"
        self.host = "localhost"
        self.port = 3306