import threading

class db_engine(object):
    def __init__(self, connect):
        self.connect = connect

    def connect(self):
        return self.connect()

class db_context(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        self.connection = lasy_connection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

class connection_context(object):
    def __enter__(self):
        global db_ctx
        self.should_cleanup = False
        if not db_ctx.is_init():
            db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, type, value, traceback):
        global db_ctx
        if self.should_cleanup:
            db_ctx.cleanup()

class connection():
    return connection_context()

engine = None
db_ctx = db_context()
