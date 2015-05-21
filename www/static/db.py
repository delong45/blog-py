import threading
import functools
import logging

def _log(s):
    logging.debug(s)

class db_engine(object):
    def __init__(self, connect):
        self.connect = connect

    def connect(self):
        return self.connect()

engine = None

db_connect = None
db_convert = '?'

def init():
    pass

class lasy_connection():
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            _log('open db connection...')
            self.connection = db_connect()
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            _log('close db connection...')
            connection.close()

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

db_ctx = db_context()

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

def connection():
    return connection_context()

def with_connection(func):
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with connection_context():
            return func(*args, **kw)
    return _wrapper

@with_connection
def select(sql, *args):
    pass

@with_connection
def update(sql, *args):
    pass

@with_connection
def insert(sql, *args):
    pass

@with_connection
def delete(sql, *args):
    pass

class transaction_context(object):
    def __enter__(self):
        global db_ctx
        self.should_close_conn = False
        if not db_ctx.is_init():
            db_ctx.init()
            self.should_close_conn = True
        db_ctx.transactions = db_ctx.transactions + 1
        return self

    def __exit__(self, type, value, traceback):
        global db_ctx
        db_ctx.transactions = db_ctx.transactions - 1
        try:
            if db_ctx.transactions == 0:
                if type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                db_ctx.cleanup()

    def commit(self):
        global db_ctx
        try:
            db_ctx.connection.commit()
        except:
            db_ctx.connection.rollback()
            raise

    def rollback(self):
        global db_ctx
        db_ctx.connection.rollback()

def transaction():
    return transaction_context() 

if __name__ == '__main__':
    print "hello mysql"
