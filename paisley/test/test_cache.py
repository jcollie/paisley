# -*- Mode: Python; test-case-name: paisley.test.test_cache -*-
# vi:si:et:sw=4:sts=4:ts=4

# Copyright (c) 2011
# See LICENSE for details.

"""
Test for couchdb client caching implementation.
"""

from twisted.internet import defer

from paisley import client

from paisley.test import util


class MemoryCacheTestCase(util.CouchDBTestCase):

    def setUp(self):
        util.CouchDBTestCase.setUp(self)

        self.cache = client.MemoryCache()
        self.db = client.CouchDB('localhost', port=self.wrapper.port,
            username='testpaisley', password='testpaisley',
            cache=self.cache)

        d = defer.Deferred()
        d.addCallback(lambda _: self.db.createDB('test'))
        d.addCallback(lambda _: self.db.saveDoc('test', {
            'key': 'value',
        }))
        d.addCallback(lambda r: setattr(self, 'first', r['id']))
        d.addCallback(lambda _: self.db.saveDoc('test', {
            'lock': 'chain',
        }))
        d.addCallback(lambda r: setattr(self, 'second', r['id']))
        d.callback(None)
        return d

    def testCached(self):

        d = defer.Deferred()

        d.addCallback(lambda _: self.db.openDoc('test', self.first))

        def openCb(result):
            self.assertEquals(result['key'], 'value')
        d.addCallback(openCb)
        d.addCallback(lambda _: self.assertEquals(self.cache.lookups, 1))
        d.addCallback(lambda _: self.assertEquals(self.cache.hits, 0))
        d.addCallback(lambda _: self.assertEquals(self.cache.cached, 1))

        d.addCallback(lambda _: self.db.openDoc('test', self.first))

        d.addCallback(openCb)
        d.addCallback(lambda _: self.assertEquals(self.cache.lookups, 2))
        d.addCallback(lambda _: self.assertEquals(self.cache.hits, 1))
        d.addCallback(lambda _: self.assertEquals(self.cache.cached, 1))

        d.addCallback(lambda _: self.db.openDoc('test', self.second))

        def openCb(result):
            self.assertEquals(result['lock'], 'chain')
        d.addCallback(openCb)
        d.addCallback(lambda _: self.assertEquals(self.cache.lookups, 3))
        d.addCallback(lambda _: self.assertEquals(self.cache.hits, 1))
        d.addCallback(lambda _: self.assertEquals(self.cache.cached, 2))

        d.callback(None)
        return d
