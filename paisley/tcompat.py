"""
HTTP client.
"""

from urllib import splithost, splittype

from twisted.web.http_headers import Headers


# copied from twisted 11.0 and slightly adapted for Headers

class _FakeUrllib2Request(object):
    """
    A fake C{urllib2.Request} object for C{cookielib} to work with.

    @see: U{http://docs.python.org/library/urllib2.html#request-objects}

    @type uri: C{str}
    @ivar uri: Request URI.

    @type headers: L{twisted.web.http_headers.Headers}
    @ivar headers: Request headers.

    @type type: C{str}
    @ivar type: The scheme of the URI.

    @type host: C{str}
    @ivar host: The host[:port] of the URI.

    @since: 11.1
    """
    def __init__(self, uri):
        self.uri = uri
        self.headers = Headers()
        self.type, rest = splittype(self.uri)
        self.host, rest = splithost(rest)


    def has_header(self, header):
        return self.headers.hasHeader(header)


    def add_unredirected_header(self, name, value):
        self.headers.addRawHeader(name, value)


    def get_full_url(self):
        return self.uri


    def get_header(self, name, default=None):
        headers = self.headers.getRawHeaders(name, default)
        if headers is not None:
            return headers[0]
        return None


    def get_host(self):
        return self.host


    def get_type(self):
        return self.type


    def is_unverifiable(self):
        # In theory this shouldn't be hardcoded.
        return False



class _FakeUrllib2Response(object):
    """
    A fake C{urllib2.Response} object for C{cookielib} to work with.

    @type response: C{twisted.web.iweb.IResponse}
    @ivar response: Underlying Twisted Web response.

    @since: 11.1
    """
    def __init__(self, response):
        self.response = response


    def info(self):
        class _Meta(object):
            def getheaders(zelf, name):
                return self.response.headers.getRawHeaders(name, [])
        return _Meta()



class CookieAgent(object):
    """
    L{CookieAgent} extends the basic L{Agent} to add RFC-compliant
    handling of HTTP cookies.  Cookies are written to and extracted
    from a C{cookielib.CookieJar} instance.

    The same cookie jar instance will be used for any requests through this
    agent, mutating it whenever a I{Set-Cookie} header appears in a response.

    @type _agent: L{twisted.web.client.Agent}
    @ivar _agent: Underlying Twisted Web agent to issue requests through.

    @type cookieJar: C{cookielib.CookieJar}
    @ivar cookieJar: Initialized cookie jar to read cookies from and store
        cookies to.

    @since: 11.1
    """
    def __init__(self, agent, cookieJar):
        self._agent = agent
        self.cookieJar = cookieJar


    def request(self, method, uri, headers=None, bodyProducer=None):
        """
        Issue a new request to the wrapped L{Agent}.

        Send a I{Cookie} header if a cookie for C{uri} is stored in
        L{CookieAgent.cookieJar}. Cookies are automatically extracted and
        stored from requests.

        If a C{'cookie'} header appears in C{headers} it will override the
        automatic cookie header obtained from the cookie jar.

        @see: L{Agent.request}
        """
        if headers is None:
            headers = Headers()
        lastRequest = _FakeUrllib2Request(uri)
        # Setting a cookie header explicitly will disable automatic request
        # cookies.
        if not headers.hasHeader('cookie'):
            self.cookieJar.add_cookie_header(lastRequest)
            cookieHeader = lastRequest.get_header('Cookie', None)
            if cookieHeader is not None:
                try:
                    headers = headers.copy()
                except:
                    # 11.0 does not have .copy
                    headers = Headers(headers._rawHeaders)
                
                headers.addRawHeader('cookie', cookieHeader)

        d = self._agent.request(method, uri, headers, bodyProducer)
        d.addCallback(self._extractCookies, lastRequest)
        return d


    def _extractCookies(self, response, request):
        """
        Extract response cookies and store them in the cookie jar.

        @type response: L{twisted.web.iweb.IResponse}
        @param response: Twisted Web response.

        @param request: A urllib2 compatible request object.
        """
        resp = _FakeUrllib2Response(response)
        self.cookieJar.extract_cookies(resp, request)
        return response
