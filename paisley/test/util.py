# -*- Mode: Python; test-case-name: paisley.test.test_util -*-
# vi:si:et:sw=4:sts=4:ts=4

# Copyright (c) 2007-2008
# See LICENSE for details.

import commands
import glob
import re
import os
import tempfile
import subprocess
import time
import commands
import ConfigParser

from twisted.trial import unittest

from paisley import client, pjson


class CouchDBWrapper(object):
    """
    I wrap an external CouchDB instance started and stopped for testing.

    @ivar tempdir: the temporary directory used for logging and running
    @ivar process: the CouchDB process
    @type process: L{subprocess.Popen}
    @ivar port:    the randomly assigned port on which CouchDB listens
    @type port:    str
    @ivar db:      the CouchDB client to this server
    @type db:      L{client.CouchDB}
    """

    def start(self):
        self.tempdir = tempfile.mkdtemp(prefix='tmp.paisley.test.')

        version = self.version()

        # compare version to the templates defined for each version, starting
        # with the most recent and ending with the default
        paths = glob.glob(
            os.path.join(os.path.dirname(__file__),
                'test.ini.template*'))
        paths.sort()
        paths.reverse()

        target = 'test.ini.template-%s' % version

        for path in paths:
            name = os.path.basename(path)
            # FIXME: in the future, if the distinction matters, use
            # distutils.version.LooseVersion instead
            if name < target:
                break

        handle = open(path)

        conf = handle.read() % {
            'tempdir': self.tempdir,
        }

        confPath = os.path.join(self.tempdir, 'test.ini')
        handle = open(confPath, 'w')
        handle.write(conf)
        handle.close()

        # create the dirs from the template
        os.mkdir(os.path.join(self.tempdir, 'lib'))
        os.mkdir(os.path.join(self.tempdir, 'log'))

        args = ['couchdb', '-a', confPath]
        self.process = subprocess.Popen(
            args, env=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # find port
        logPath = os.path.join(self.tempdir, 'log', 'couch.log')
        while not os.path.exists(logPath):
            if self.process.poll() is not None:
                raise Exception("""
couchdb exited with code %d.
stdout:
%s
stderr:
%s""" % (
                    self.process.returncode, self.process.stdout.read(),
                    self.process.stderr.read()))
            time.sleep(0.01)

        while os.stat(logPath).st_size == 0:
            time.sleep(0.01)

        PORT_RE = re.compile(
            'Apache CouchDB has started on http://127.0.0.1:(?P<port>\d+)')

        handle = open(logPath)
        line = handle.read()
        m = PORT_RE.search(line)
        if not m:
            self.stop()
            raise Exception("Cannot find port in line %s" % line)

        self.port = int(m.group('port'))
        self.db = client.CouchDB(host='localhost', port=self.port,
            username='testpaisley', password='testpaisley')

    def stop(self):
        self.process.terminate()

        os.system("rm -rf %s" % self.tempdir)

    def version(self):
        (status, output) = commands.getstatusoutput('couchdb -V')
        assert status == 0, 'Could not run couchdb -V: %r' % (output, )

        # couchdb - Apache CouchDB 1.4.0+build.9b64526
        VERSION_RE = re.compile(
            '^couchdb - Apache CouchDB (?P<version>.*)$')

        m = VERSION_RE.search(output.split('\n')[0])

        return m.group('version')

class CouchDBConfig(object):
    """
    I parse couchdb configs.

    @ivar parser: a config parser, loaded with couchdb's configuration
    @type parser: L{ConfigParser.ConfigParser}
    """

    def __init__(self):
        output = commands.getoutput('couchdb -c')
        paths = output.strip().split('\n')
        self.parser = ConfigParser.ConfigParser()
        self.parser.read(paths)

    def query_server(self, language):
        for l, argString in self.parser.items('query_servers'):
            if l == language:
                return argString

        raise KeyError('No query_server found for %s' % language)


class QueryServerException(Exception):

    def __init__(self, output):
        self.args = (output, )
        self.output = output


class JSQueryServerException(QueryServerException):

    def __init__(self, output, exceptionType, message, line, docId):
        self.args = (output, exceptionType, message, line, docId)
        self.output = output
        self.exceptionType = exceptionType
        self.message = message
        self.line = line
        self.docId = docId


class CouchQSWrapper(object):
    """
    I wrap an external CouchDB query server instance started and stopped for
    testing.

    See http://wiki.apache.org/couchdb/View_server

    @ivar language: the language this query server is for
    @ivar process:  the CouchDB query server process
    @type process:  L{subprocess.Popen}
    """
    language = None

    def __init__(self):
        self.config = CouchDBConfig()
        self.args = self.config.query_server(self.language).split()

    def start(self):
        try:
            self.process = subprocess.Popen(
                self.args, env=None,
                close_fds=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError:
            print 'Could not start', self.args
            raise


    ### query_server protocol implementation
    #   these methods implement the basic primitives, stripping the
    #   trailing newline from output

    def reset(self):
        self.process.stdin.write('["reset"]\n')
        out = self.process.stdout.readline()
        if out != 'true\n':
            raise QueryServerException((out, ))

    def add_fun(self, func):
        func = self.processFunction(func)

        func = '\\n'.join(func.split('\n'))

        self.process.stdin.write('["add_fun", "%s"]\n' % func)
        out = self.process.stdout.readline()
        if out != 'true\n':
            raise QueryServerException((out, ))

    def map_doc(self, doc):
        doc = ' '.join(doc.split('\n'))
        self.process.stdin.write('["map_doc", %s]\n' % doc)
        out = self.process.stdout.readline().strip()
        self.parseException(out)

        return out

    def reduce(self, func, results):
        """
        @param results: comma-seperated list of [[key, id-of-doc], value]
        @type  results: str
        """
        func = self.processFunction(func)

        func = '\\n'.join(func.split('\n'))

        line = '["reduce", ["%s"], %s]\n' % (func, results)
        self.process.stdin.write(line)
        out = self.process.stdout.readline().strip()
        self.parseException(out)

        return out

    def rereduce(self, func, values):
        """
        @param values: comma-seperated list of values
        @type  values: str
        """
        func = self.processFunction(func)

        func = '\\n'.join(func.split('\n'))

        line = '["rereduce", ["%s"], %s]\n' % (func, values)
        self.process.stdin.write(line)
        out = self.process.stdout.readline().strip()
        self.parseException(out)

        return out

    ### methods for subclasses

    def parseException(self, line):
        pass

    def processFunction(self, func):
        """
        Process the given text input for a function.

        This lets implementation expand functions before sending them.

        @type  func: the function to process

        @rtype: C{str}
        """
        pass

    ### public interface

    def map(self, func, doc):
        """
        Map the given document with the given function.

        @param func: the source code for the map function.
        @type  func: C{str}
        @type  doc:  the document to map
        @type  doc:  C{unicode}

        @rtype: C{unicode}
        """
        self.reset()
        self.add_fun(func)

        return self.map_doc(doc)

    def mapreduce(self, mapFunc, reduceFunc, docs):
        self.reset()
        self.add_fun(mapFunc)

        ret = []
        for doc in docs:
            ret.append(self.map_doc(doc))

        return self.reduce(reduceFunc, ret)

    def stop(self):
        self.process.terminate()


class CouchJSWrapper(CouchQSWrapper):
    """
    """

    language = 'javascript'
    path = None

    ### subclass implementations

    def parseException(self, line):
        obj = pjson.loads(line)
        if isinstance(obj, list):
            if obj[0] == 'log':
                message = obj[1]
                self.parseMessage(message)

    def parseMessage(self, message):
        import re
        matcher = re.compile(r'''
            ^function\sraised\sexception\s\(new\s
            (?P<type>\w*)
            \(
            "(?P<message>[^"]*)",\s
            "(?P<debug>[^"]*)",\s
            (?P<line>\d*)\)\)\s
            with\sdoc._id\s
            (?P<doc_id>.*)$
        ''', re.VERBOSE)
        m = matcher.search(message)
        if m:
            raise JSQueryServerException(output=message,
                exceptionType=m.group('type'),
                message=m.group('message'),
                line=int(m.group('line')),
                docId=m.group('doc_id'))

    def processFunction(self, func):
        # process using couchapp if it exists
        # this lets us use !code directives
        try:
            from couchapp import macros
            func = macros.run_code_macros(func, self.path)
            return func
        except ImportError:
            pass

        return func

    ### public API

    def mapPath(self, path, doc):
        """
        Map the given document with the map function in the given path.
        """
        fullPath = os.path.join(self.path, path)
        func = open(fullPath).read()

        return self.map(func, doc)

    def mapReducePath(self, mapPath, reducePath, docs):
        """
        Map the given documents with the map/reduce functions in the given
        path.
        """
        fullMapPath = os.path.join(self.path, mapPath)
        mapFunc = open(fullMapPath).read()
        fullReducePath = os.path.join(self.path, reducePath)
        reduceFunc = open(fullReducePath).read()

        return self.mapreduce(mapFunc, reduceFunc, docs)


class CouchQSTestCase(unittest.TestCase):
    """
    I am a TestCase base class for tests against a real CouchDB view server.
    I start a server during setup and stop it during teardown.
    """

    language = 'javascript'
    QueryServerClass = CouchQSWrapper

    def setUp(self):
        self.wrapper = self.QueryServerClass()
        self.wrapper.start()

    def tearDown(self):
        self.wrapper.stop()


class CouchDBTestCase(unittest.TestCase):
    """
    I am a TestCase base class for tests against a real CouchDB server.
    I start a server during setup and stop it during teardown.

    @ivar  db: the CouchDB client
    @type  db: L{client.CouchDB}
    """

    def setUp(self):
        self.wrapper = CouchDBWrapper()
        self.wrapper.start()
        self.db = self.wrapper.db

    def tearDown(self):
        self.wrapper.stop()

    # helper callbacks

    def checkDatabaseEmpty(self, result):
        self.assertEquals(result['rows'], [])
        self.assertEquals(result['total_rows'], 0)
        self.assertEquals(result['offset'], 0)

    def checkInfoNewDatabase(self, result):
        self.assertEquals(result['update_seq'], 0)
        self.assertEquals(result['purge_seq'], 0)
        self.assertEquals(result['doc_count'], 0)
        self.assertEquals(result['db_name'], 'test')
        self.assertEquals(result['doc_del_count'], 0)
        self.assertEquals(result['committed_update_seq'], 0)

    def checkResultOk(self, result):
        self.assertEquals(result, {'ok': True})

    def checkResultEmptyView(self, result):
        self.assertEquals(result['rows'], [])
        self.assertEquals(result['total_rows'], 0)
        self.assertEquals(result['offset'], 0)


def eight_bit_test_string():
    return ''.join(chr(cn) for cn in xrange(0x100)) * 2
