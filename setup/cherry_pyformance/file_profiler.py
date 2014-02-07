import __builtin__
import time
from cherry_pyformance import cfg, stat_logger
import os



file_stats_buffer = {}


class FileWrapper(object):

    def __init__(self, file, datetime, time_to_open):
        self.open_time = time.clock()
        self.file = file
        self.fullname = os.path.abspath(file.name)
        try:
            self.relname = os.path.relpath(file.name)
        except:
            self.relname = file.name
        self.mode = file.mode
        self.datetime = datetime
        self.time_to_open = time_to_open
        self.written = 0
        self.closed = False
        self.encoding = file.encoding
        self.errors = file.errors
        self.newlines = file.newlines
        self.softspace = file.softspace

    def __enter__(self):
        return self

    def seek(self, offset, whence=None):
        return self.file.seek(offset, whence) if whence else self.file.seek(offset)

    def read(self, size=None):
        return self.file.read(size) if size else self.file.read()

    def readline(self, size=None):
        return self.file.readline(size) if size else self.file.readline()

    def readlines(self, sizehint=None):
        return self.file.readlines(sizehint) if sizehint else self.file.readlines()

    def write(self, string):
        self.file.write(string)
        self.written += len(string)

    def writelines(self, seq):
        self.file.writelines(seq)
        for line in seq:
            self.written += len(line)

    def tell(self):
        return self.file.tell()
    def flush(self):
        return self.file.flush()
    def fileno(self):
        return self.file.fileno()
    def istty(self):
        return self.file.istty()
    def next(self):
        return self.file.next()
    def truncate(self, size=None):
        return self.file.truncate(size) if size else self.file.truncate()


    def close(self):
        self.__exit__()

    def __exit__(self, *args, **kwargs):
        self.close_time = time.clock()
        self.file.close()
        if 'ignored_directories' in cfg['files'] and cfg['files']['ignored_directories']:
            for file_path in cfg['files']['ignored_directories'].split(','):
                if file_path in self.fullname.replace('\\','/'):
                    return
        file_stats_buffer[id(self)] = {'datetime':self.datetime,
                                       'duration':self.close_time-self.open_time,
                                       'time_to_open':self.time_to_open,
                                       'data_written':self.written,
                                       'filename':self.relname,
                                       'mode':self.mode}


class OpenFn(object):

    def __init__(self, old_open):
        self.old_open = old_open

    def __call__(self, filename, mode='r'):
        datetime = time.time()
        before_open = time.clock()
        f = self.old_open(filename, mode)
        time_to_open = time.clock() - before_open
        return FileWrapper(f, datetime, time_to_open)

def decorate_open():
    _open = __builtin__.open
    stat_logger.info('Wrapping file access functions')
    __builtin__.open = OpenFn(_open)


# if __name__ == '__main__':
#     # need to comment cherry_pyformance lines before testing
#     file_stats_buffer = []
#     fn = 'text.txt'
#     decorate_open()
#     x = open(fn,'w')
#     x.write('hey this is a test')
#     x.close()
#     y = open(fn,'r')
#     print y.read()
#     y.close()
#     os.remove(fn)
#     import pprint
#     pprint.pprint(file_stats_buffer)
