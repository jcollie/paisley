# -*- Mode: Python; test-case-name: paisley.test.test_util -*-
# vi:si:et:sw=4:sts=4:ts=4

from twisted.trial import unittest

from paisley.test import util


class ConfigParserTestCase(unittest.TestCase):

    def setUp(self):
        self.config = util.CouchDBConfig()

    def test_getQueryServers(self):
        servers = self.config.parser.items('query_servers')

        # there is no guarantee someone is using couchjs for javascript,
        # but this seems like the least breakable way to test
        for name, arg in servers:
            if name == 'javascript':
                self.failUnless('couchjs' in arg)


class CouchJSQueryTestCase(util.CouchQSTestCase):
    """
    I am a base class for tests.
    """
    QueryServerClass = util.CouchJSWrapper

    def test_parseMessage(self):
        message = "function raised exception (new TypeError(\"" + \
            " doc.fragments is undefined\", \"\", 18))" + \
            " with doc._id 8877AFF9789988EE"

        try:
            self.wrapper.parseMessage(message)
        except util.JSQueryServerException, e:
            self.assertEquals(e.line, 18)
            self.assertEquals(e.docId, u'8877AFF9789988EE')

    def test_map(self):
        func = """
function(doc) {
    if(doc.score > 50)
        emit(null, {'player_name': doc.name});
}
"""
        doc = '''
{
    "_id":"8877AFF9789988EE",
    "_rev":"3-235256484",
    "name":"John Smith",
    "score": 60
}'''
        out = self.wrapper.map(func, doc)
        self.assertEquals(out, '[[[null,{"player_name":"John Smith"}]]]')

        doc = '''
{
    "_id":"9590AEB4585637FE",
    "_rev":"1-674684684",
    "name":"Jane Parker",
    "score": 43
}'''
        out = self.wrapper.map(func, doc)
        self.assertEquals(out, '[[]]')

    def test_reduce(self):
        func = "function(k, v) { return sum(v); }"
        results = """
[
    [[1,"699b524273605d5d3e9d4fd0ff2cb272"],10],
    [[2,"c081d0f69c13d2ce2050d684c7ba2843"],20],
    [[null,"foobar"],3]
]"""
        out = self.wrapper.reduce(func, "".join(results.split()))
        self.assertEquals(out, '[true,[33]]')

    def test_rereduce(self):
        func = "function(k, v, r) { return sum(v); }"
        results = "[33,55,66]"
        out = self.wrapper.rereduce(func, results)
        self.assertEquals(out, '[true,[154]]')
