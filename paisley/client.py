# -*- Mode: Python; test-case-name: paisley.test.test_client -*-
# vi:si:et:sw=4:sts=4:ts=4

# Copyright (c) 2007-2008
# See LICENSE for details.

"""
CouchDB client.
"""

import json

from encodings import utf_8
import logging
import types
import cookielib

from urllib import urlencode, quote
from zope.interface import implements

from twisted.internet import defer
from twisted.internet import defer, task
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer

from twisted.internet.defer import Deferred, maybeDeferred
from twisted.internet.protocol import Protocol

try:
    from base64 import b64encode
except ImportError:
    import base64

    def b64encode(s):
        return "".join(base64.encodestring(s).split("\n"))


# for quoting database names, we also need to encode slashes
def _namequote(name):
    return quote(name, safe='')

def short_print(body, trim=255):
    # don't go nuts on possibly huge log entries
    # since we're a library we should try to avoid calling this and instead
    # write awesome logs
    if not isinstance(body, basestring):
        body = str(body)
    if len(body) < trim:
        return body.replace('\n', '\\n')
    else:
        return body[:trim].replace('\n', '\\n') + '...'

try:
    from functools import partial
except ImportError:

    class partial(object):

        def __init__(self, fn, *args, **kw):
            self.fn = fn
            self.args = args
            self.kw = kw

        def __call__(self, *args, **kw):
            if kw and self.kw:
                d = self.kw.copy()
                d.update(kw)
            else:
                d = kw or self.kw
            return self.fn(*(self.args + args), **d)

SOCK_TIMEOUT = 300


class StringProducer(object):
    """
    Body producer for t.w.c.Agent
    """
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        return maybeDeferred(consumer.write, self.body)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class ResponseReceiver(Protocol):
    """
    Assembles HTTP response from return stream.
    """

    def __init__(self, deferred, decode_utf8):
        self.recv_chunks = []
        self.decoder = utf_8.IncrementalDecoder() if decode_utf8 else None
        self.deferred = deferred

    def dataReceived(self, bytes, final=False):
        if self.decoder:
            bytes = self.decoder.decode(bytes, final)
        self.recv_chunks.append(bytes)

    def connectionLost(self, reason):
        # _newclient and http import reactor
        from twisted.web._newclient import ResponseDone
        from twisted.web.http import PotentialDataLoss

        if reason.check(ResponseDone) or reason.check(PotentialDataLoss):
            self.dataReceived('', final=True)
            self.deferred.callback(''.join(self.recv_chunks))
        else:
            self.deferred.errback(reason)


class CouchDB(object):
    """
    CouchDB client: hold methods for accessing a couchDB.
    """

    def __init__(self, host, port=5984, dbName=None,
                 username=None, password=None, protocol='http',
                 disable_log=False,
                 version=(1, 0, 1), cache=None):
        """
        Initialize the client for given host.

        @param host:     address of the server.
        @type  host:     C{str}
        @param port:     if specified, the port of the server.
        @type  port:     C{int}
        @param dbName:   if specified, all calls needing a database name will
                         use this one by default.
                         Note that only lowercase characters (a-z), digits
                         (0-9), or any of the characters _, $, (, ), +, -, and
                         / are allowed.
        @type  dbName:   C{str}
        @param username: the username
        @type  username: C{unicode}
        @param password: the password
        @type  password: C{unicode}
        """
        if disable_log:
            # since this is the db layer, and we generate a lot of logs,
            # let people disable them completely if they want to.
            levels = ['trace', 'debug', 'info', 'warn', 'error', 'exception']

            class FakeLog(object):
                pass

            def nullfn(self, *a, **k):
                pass
            self.log = FakeLog()
            for level in levels:
                self.log.__dict__[level] = types.Methodtype(nullfn, self.log)
        else:
            self.log = logging.getLogger('paisley')

        from twisted.internet import reactor
        # t.w.c imports reactor
        from twisted.web.client import Agent
        try:
            from twisted.web.client import CookieAgent
            self.log.debug('using twisted.web.client.CookieAgent')
        except:
            from paisley.tcompat import CookieAgent
            self.log.debug('using paisley.tcompat.CookieAgent')

        agent = Agent(reactor)
        self.client = CookieAgent(agent, cookielib.CookieJar())
        self.host = host
        self.port = int(port)
        if isinstance(username, str):
            username = unicode(username)
        self.username = username
        if isinstance(password, str):
            password = unicode(password)
        self.password = password
        self._cache = cache
        self._authenticator = None
        self._authLC = None # looping call to keep us authenticated
        self._session = {}

        self.url_template = "%s://%s:%s%%s" % (protocol, self.host, self.port)

        if dbName is not None:
            self.bindToDB(dbName)


        self.log.debug("[%s%s:%s/%s] init new db client",
                       '%s@' % (username, ) if username else '',
                       host,
                       port,
                       dbName if dbName else '')
        self.version = version

    def parseResult(self, result):
        """
        Parse JSON result from the DB.
        """
        return json.loads(result)

    def bindToDB(self, dbName):
        """
        Bind all operations asking for a DB name to the given DB.
        """
        for methname in ["createDB", "deleteDB", "infoDB", "listDoc",
                         "openDoc", "saveDoc", "deleteDoc", "openView",
                         "tempView"]:
            method = getattr(self, methname)
            newMethod = partial(method, dbName)
            setattr(self, methname, newMethod)

    # Database operations

    def createDB(self, dbName):
        """
        Creates a new database on the server.

        @type  dbName: str
        """
        # Responses: {u'ok': True}, 409 Conflict, 500 Internal Server Error,
        # 401 Unauthorized
        # 400 {"error":"illegal_database_name","reason":"Only lowercase
        # characters (a-z), digits (0-9), and any of the characters _, $, (,
        # ), +, -, and / are allowed. Must begin with a letter."}

        return self.put("/%s/" % (_namequote(dbName), ), "", descr='CreateDB'
            ).addCallback(self.parseResult)

    def cleanDB(self, dbName):
        """
        Clean old view indexes for the database on the server.

        @type  dbName: str
        """
        # Responses: 200, 404 Object Not Found
        return self.post("/%s/_view_cleanup" % (_namequote(dbName), ), "",
        descr='cleanDB'
            ).addCallback(self.parseResult)

    def compactDB(self, dbName):
        """
        Compacts the database on the server.

        @type  dbName: str
        """
        # Responses: 202 Accepted, 404 Object Not Found
        return self.post("/%s/_compact" % (_namequote(dbName), ), "",
            descr='compactDB'
            ).addCallback(self.parseResult)

    def compactDesignDB(self, dbName, designName):
        """
        Compacts the database on the server.

        @type  dbName: str
        @type  designName: str
        """
        # Responses: 202 Accepted, 404 Object Not Found
        return self.post("/%s/_compact/%s" % (_namequote(dbName), designName),
            "", descr='compactDesignDB'
            ).addCallback(self.parseResult)


    def deleteDB(self, dbName):
        """
        Deletes the database on the server.

        @type  dbName: str
        """
        # Responses: {u'ok': True}, 404 Object Not Found
        return self.delete("/%s/" % (_namequote(dbName), )
            ).addCallback(self.parseResult)

    def listDB(self):
        """
        List the databases on the server.
        """
        # Responses: list of db names
        return self.get("/_all_dbs", descr='listDB').addCallback(
            self.parseResult)

    def getVersion(self):
        """
        Returns the couchDB version.
        """
        # Responses: {u'couchdb': u'Welcome', u'version': u'1.1.0'}
        # Responses: {u'couchdb': u'Welcome', u'version': u'1.1.1a1162549'}
        d = self.get("/", descr='version').addCallback(self.parseResult)

        def cacheVersion(result):
            self.version = self._parseVersion(result['version'])
            return result
        return d.addCallback(cacheVersion)

    def _parseVersion(self, versionString):

        def onlyInt(part):
            import re
            intRegexp = re.compile("^(\d+)")
            m = intRegexp.search(part)
            if not m:
                return None
            return int(m.expand('\\1'))

        ret = tuple(onlyInt(_) for _ in versionString.split('.'))
        return ret

    def infoDB(self, dbName):
        """
        Returns info about the couchDB.
        """
        # Responses: {u'update_seq': 0, u'db_name': u'mydb', u'doc_count': 0}
        # 404 Object Not Found
        return self.get("/%s/" % (_namequote(dbName), ), descr='infoDB'
            ).addCallback(self.parseResult)

    # Document operations

    def listDoc(self, dbName, reverse=False, startkey=None, endkey=None,
                include_docs=False, limit=-1, **obsolete):
        """
        List all documents in a given database.
        """
        # Responses: {u'rows': [{u'_rev': -1825937535, u'_id': u'mydoc'}],
        # u'view': u'_all_docs'}, 404 Object Not Found
        import warnings
        if 'count' in obsolete:
            warnings.warn("listDoc 'count' parameter has been renamed to "
                          "'limit' to reflect changing couchDB api",
                          DeprecationWarning)
            limit = obsolete.pop('count')
        if obsolete:
            raise AttributeError("Unknown attribute(s): %r" % (
                obsolete.keys(), ))
        uri = "/%s/_all_docs" % (_namequote(dbName), )
        args = {}
        if reverse:
            args["reverse"] = "true"
        if startkey:
            args["startkey"] = json.dumps(startkey)
        if endkey:
            args["endkey"] = json.dumps(endkey)
        if include_docs:
            args["include_docs"] = True
        if limit >= 0:
            args["limit"] = int(limit)
        if args:
            uri += "?%s" % (urlencode(args), )
        return self.get(uri, descr='listDoc').addCallback(self.parseResult)

    def openDoc(self, dbName, docId, revision=None, full=False, attachment=""):
        """
        Open a document in a given database.

        @type docId: C{unicode}

        @param revision: if specified, the revision of the document desired.
        @type revision: C{unicode}

        @param full: if specified, return the list of all the revisions of the
            document, along with the document itself.
        @type full: C{bool}

        @param attachment: if specified, return the named attachment from the
            document.
        @type attachment: C{str}
        """
        # FIXME: where should we enforce unicode ?
        docId = unicode(docId)
        # Responses: {u'_rev': -1825937535, u'_id': u'mydoc', ...}
        # 404 Object Not Found

        # FIXME: remove these conversions and have our callers do them
        docId = unicode(docId)
        assert type(docId) is unicode, \
            'docId is %r instead of unicode' % (type(docId), )

        if revision:
            revision = unicode(revision)
            assert type(revision) is unicode, \
                'revision is %r instead of unicode' % (type(revision), )

        docIdUri = docId.encode('utf-8')
        # on special url's like _design and _local no slash encoding is needed,
        # and doing so would hit a 301 redirect
        if not docIdUri.startswith('_'):
            docIdUri = _namequote(docIdUri)

        uri = "/%s/%s" % (_namequote(dbName), docIdUri)
        if revision is not None:
            uri += "?%s" % (urlencode({"rev": revision.encode('utf-8')}), )
        elif full:
            uri += "?%s" % (urlencode({"full": "true"}), )
        elif attachment:
            uri += "/%s" % quote(attachment)
            # No parsing
            return self.get(uri, descr='openDoc', isJson=False)

        # just the document
        if self._cache:
            try:
                return self._cache.get(docId)
            except:
                pass

        return self.get(uri, descr='openDoc').addCallback(
            self.parseResult).addCallback(
            self._cacheResult, docId)

    def _cacheResult(self, value, docId):
        if self._cache:
            self._cache.store(docId, value)

        return value

    def addAttachments(self, document, attachments):
        """
        Add attachments to a document, before sending it to the DB.

        @param document: the document to modify.
        @type document: C{dict}

        @param attachments: the attachments to add.
        @type attachments: C{dict}
        """
        document.setdefault("_attachments", {})
        for name, data in attachments.iteritems():
            data = b64encode(data)
            document["_attachments"][name] = {"type": "base64", "data": data}

    def saveDoc(self, dbName, body, docId=None):
        """
        Save/create a document to/in a given database.

        @param dbName: identifier of the database.
        @type dbName: C{str}

        @param body: content of the document.
        @type body: C{str} or any structured object

        @param docId: if specified, the identifier to be used in the database.
        @type docId: C{unicode}
        """
        # Responses: {'rev': '1-9dd776365618752ddfaf79d9079edf84',
        #             'ok': True, 'id': '198abfee8852816bc112992564000295'}

        # 404 Object not found (if database does not exist)
        # 409 Conflict, 500 Internal Server Error
        if docId:
            # FIXME: remove these conversions and have our callers do them
            docId = unicode(docId)
            assert type(docId) is unicode, \
                'docId is %r instead of unicode' % (type(docId), )

        if not isinstance(body, (str, unicode)):
            body = json.dumps(body)
        if docId is not None:
            d = self.put("/%s/%s" % (_namequote(dbName),
                _namequote(docId.encode('utf-8'))),
                body, descr='saveDoc')
        else:
            d = self.post("/%s/" % (_namequote(dbName), ), body,
                descr='saveDoc')
        return d.addCallback(self.parseResult)

    def deleteDoc(self, dbName, docId, revision):
        """
        Delete a document on given database.

        @param dbName:   identifier of the database.
        @type  dbName:   C{str}

        @param docId:    the document identifier to be used in the database.
        @type  docId:    C{unicode}

        @param revision: the revision of the document to delete.
        @type  revision: C{unicode}

        """
        # Responses: {u'_rev': 1469561101, u'ok': True}
        # 500 Internal Server Error

        docId = unicode(docId)
        assert type(docId) is unicode, \
            'docId is %r instead of unicode' % (type(docId), )

        revision = unicode(revision)
        assert type(revision) is unicode, \
            'revision is %r instead of unicode' % (type(revision), )


        return self.delete("/%s/%s?%s" % (
                _namequote(dbName),
                _namequote(docId.encode('utf-8')),
                urlencode({'rev': revision.encode('utf-8')}))).addCallback(
                    self.parseResult)

    # View operations

    def openView(self, dbName, docId, viewId, **kwargs):
        """
        Open a view of a document in a given database.
        """
        # Responses:
        # 500 Internal Server Error (illegal database name)

        def buildUri(dbName=dbName, docId=docId, viewId=viewId, kwargs=kwargs):
            return "/%s/_design/%s/_view/%s?%s" % (
                _namequote(dbName), _namequote(docId.encode('utf-8')),
                viewId, urlencode(kwargs))

        # if there is a "keys" argument, remove it from the kwargs
        # dictionary now so that it doesn't get double JSON-encoded
        body = None
        if "keys" in kwargs:
            body = json.dumps({"keys": kwargs.pop("keys")})

        # encode the rest of the values with JSON for use as query
        # arguments in the URI
        for k, v in kwargs.iteritems():
            if k == 'keys': # we do this below, for the full body
                pass
            else:
                kwargs[k] = json.dumps(v)
        # we keep the paisley API, but couchdb uses limit now
        if 'count' in kwargs:
            kwargs['limit'] = kwargs.pop('count')

        # If there's a list of keys to send, POST the
        # query so that we can upload the keys as the body of
        # the POST request, otherwise use a GET request
        if body:
            return self.post(
                buildUri(), body=body, descr='openView').addCallback(
                    self.parseResult)
        else:
            return self.get(
                buildUri(), descr='openView').addCallback(
                    self.parseResult)

    def addViews(self, document, views):
        """
        Add views to a document.

        @param document: the document to modify.
        @type document: C{dict}

        @param views: the views to add.
        @type views: C{dict}
        """
        document.setdefault("views", {})
        for name, data in views.iteritems():
            document["views"][name] = data

    def tempView(self, dbName, view):
        """
        Make a temporary view on the server.
        """
        if not isinstance(view, (str, unicode)):
            view = json.dumps(view)
        d = self.post("/%s/_temp_view" % (_namequote(dbName), ), view,
            descr='tempView')
        return d.addCallback(self.parseResult)

    def getSession(self):
        """
        Get a session from the server using the supplied credentials.
        """
        self.log.debug("[%s:%s%s] POST %s",
                       self.host, self.port, '_session', 'getSession')
        postdata = "name=%s&password=%s" % (
                self.username.encode('utf-8'),
                self.password.encode('utf-8'))
        self.log.debug("[%s:%s%s] POST data %s",
                       self.host, self.port, '_session', 'getSession')
        d = self._getPage("/_session", method="POST",
            postdata=postdata,
            isJson=False,
            headers={
                'Content-Type': ['application/x-www-form-urlencodeddata', ],
                'Accept': ['*/*', ],
            })
        d.addCallback(self.parseResult)

        def getSessionCb(result):
            # save the response of getSession, including roles
            # {u'ok': True, u'name': u'user/thomas@apestaart.org', u'roles': [u'xbnjwxg', u'confirmed', u'hoodie:read:user/xbnjwxg', u'hoodie:write:user/xbnjwxg']}
            self.log.debug("[%s:%s%s] POST result %r",
                       self.host, self.port, '_session', result)
            self._session = result
            return result
        d.addCallback(getSessionCb)

        return d

    def getSessionRoles(self):
        """
        @rtype: C{list} of C{unicode}
        """
        if self._session:
            return self._session['roles']

        return []

    # Basic http methods

    def _getPage(self, uri, method="GET", postdata=None, headers=None,
            isJson=True):
        """
        C{getPage}-like.
        """

        def cb_recv_resp(response):
            d_resp_recvd = Deferred()
            content_type = response.headers.getRawHeaders('Content-Type',
                    [''])[0].lower().strip()
            decode_utf8 = 'charset=utf-8' in content_type or \
                    content_type == 'application/json'
            response.deliverBody(ResponseReceiver(d_resp_recvd,
                decode_utf8=decode_utf8))
            return d_resp_recvd.addCallback(cb_process_resp, response)

        def cb_process_resp(body, response):
            # twisted.web.error imports reactor
            from twisted.web import error as tw_error

            # Emulate HTTPClientFactory and raise t.w.e.Error
            # and PageRedirect if we have errors.
            if response.code > 299 and response.code < 400:
                raise tw_error.PageRedirect(response.code, body)

            # When POST'ing to replicate, CouchDB can return 404
            # instead of 401, with error: unauthorized in the body
            if response.code in [401, 404]:
                error = None
                if response.code == 404:
                    try:
                        b = json.loads(body)
                        error = b['error']
                    except:
                        pass

                if response.code == 401 or error == 'unauthorized':
                    if self._authenticator:
                        self.log.debug("401, authenticating")
                        d = self._authenticator.authenticate(self)
                        d.addCallback(lambda _: self._startLC())
                        d.addCallback(lambda _: self._getPage(
                            uri, method, postdata, headers, isJson))
                        return d

            if response.code > 399:
                raise tw_error.Error(response.code, body)

            return body

        uurl = unicode(self.url_template % (uri, ))
        url = uurl.encode('utf-8')

        if not headers:
            headers = {}

        if isJson:
            headers["Accept"] = ["application/json"]
            headers["Content-Type"] = ["application/json"]

        headers["User-Agent"] = ["paisley"]

        url = str(self.url_template % (uri,))

        if self.username:
            headers["Authorization"] = ["Basic %s" % b64encode(
                "%s:%s" % (self.username, self.password))]

        body = StringProducer(postdata) if postdata else None

        d = self.client.request(method, url, Headers(headers), body)

        d.addCallback(cb_recv_resp)

        return d

    def _startLC(self):
        self.log.debug("startLC")
        # start a looping call to keep us authenticated with cookies
        if self._authLC:
            self._authLC.stop()

        def loop():
            self.log.debug('looping authentication')
            self.get('')

        # FIXME: can we query this value instead ?
        AUTH_WINDOW = 300 # half of default
        self._authLC = task.LoopingCall(loop)
        self._authLC.start(AUTH_WINDOW)

    def get(self, uri, descr='', isJson=True):
        """
        Execute a C{GET} at C{uri}.
        """
        self.log.debug("[%s:%s%s] GET %s",
                       self.host, self.port, short_print(uri), descr)
        return self._getPage(uri, method="GET", isJson=isJson)

    def post(self, uri, body, descr=''):
        """
        Execute a C{POST} of C{body} at C{uri}.
        """
        self.log.debug("[%s:%s%s] POST %s: %s",
                      self.host, self.port, short_print(uri), descr,
                      short_print(repr(body)))
        return self._getPage(uri, method="POST", postdata=body)

    def put(self, uri, body, descr=''):
        """
        Execute a C{PUT} of C{body} at C{uri}.
        """
        self.log.debug("[%s:%s%s] PUT %s: %s",
                       self.host, self.port, short_print(uri), descr,
                       short_print(repr(body)))
        return self._getPage(uri, method="PUT", postdata=body)

    def delete(self, uri, descr=''):
        """
        Execute a C{DELETE} at C{uri}.
        """
        self.log.debug("[%s:%s%s] DELETE %s",
                       self.host, self.port, short_print(uri), descr)
        return self._getPage(uri, method="DELETE")

    # map to an object

    def map(self, dbName, docId, objectFactory, *args, **kwargs):
        """
        @type docId: unicode
        """
        # return cached version if in cache
        assert type(docId) is unicode, 'docId %r is not unicode' % docId

        try:
            return self._cache.getObject(docId)
        except (KeyError, AttributeError):
            # KeyError when docId does not exist
            # AttributeError when we don't have a cache
            d = self.openDoc(dbName, docId)

            def cb(doc):
                obj = objectFactory(*args, **kwargs)
                obj.fromDict(doc)
                self.mapped(docId, obj)
                return obj
            d.addCallback(cb)
            return d

    def mapped(self, key, obj):
        if self._cache:
            self._cache.mapped(key, obj)


class Cache(object):

    def store(key, value, operation='post'):
        """
        Store a key/value pair in the cache.

        @param key:   key to store value under
        @type  key:   C{unicode}
        @param value: the value to be stored
        @type  value: C{object}

        @rtype:   L{defer.Deferred}
        @returns: a deferred firing the value on success.
        """
        raise NotImplementedError

    def get(key):
        """
        Retrieve a key/value pair from the cache.

        @param key:   key to retrieve value with
        @type  key:   C{unicode}


        @rtype:   L{defer.Deferred}
        @returns: a deferred firing the value.
        """
        raise NotImplementedError

    def getObject(key):
        """
        Retrieve a key/object pair from the cache.

        @param key:   key to retrieve value with
        @type  key:   C{unicode}

        @rtype:   L{defer.Deferred}
        @returns: a deferred firing the value.
        """
        raise NotImplementedError

    def delete(key):
        """
        Remove a key/value pair from the cache.

        @param key:   key to delete value for
        @type  key:   C{unicode}

        @rtype:   L{defer.Deferred}
        @returns: a deferred firing True on sucess.
        """
        raise NotImplementedError

    # FIXME: can I rewrite this so that whether or not we map is pluggable ?

    def mapped(self, key, obj):
        raise NotImplementedError

    def getMapped(self, key):
        raise NotImplementedError


class MemoryCache(Cache):
    """
    I cache parsed docs in memory.
    """

    def __init__(self, docs=True, objects=True):
        self._docCache = {} # dict of dbName to dict of id to doc
        self._objCache = {} # dict of dbName to dict of id to doc

        self.lookups = 0
        self.hits = 0
        self.cached = 0

        self._docs = True
        self._objects = True

    def mapped(self, key, obj):
        assert type(key) is unicode, 'key %r is not unicode' % key
        assert type(obj) is not defer.Deferred
        if not self._objects:
            return

        if not key in self._objCache:
            self._objCache[key] = obj
            self.cached += 1

    def store(self, key, value, operation='post'):
        assert type(key) is unicode, 'key %r is not unicode' % key
        self._docCache[key] = value
        self.cached += 1
        return defer.succeed(True)

    def get(self, key):
        self.lookups += 1
        ret = self._docCache[key]
        self.hits += 1
        return defer.succeed(ret)

    def getObject(self, key):
        self.lookups += 1
        ret = self._objCache[key]
        self.hits += 1
        return defer.succeed(ret)

    def delete(self, key):
        deleted = False
        for d in [self._docCache, self._objCache]:
            try:
                del d[key]
                deleted = True
            except:
                pass
        if deleted:
            self.cached -= 1
        return defer.succeed(True)


class AuthenticationError(Exception):
    pass

class Authenticator(object):
    def authenticate(self, client):
        """
        @rtype L{defer.Deferred}
        """
        raise NotImplementedError
