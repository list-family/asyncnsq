import asyncio
import json

import aiohttp
from .http_exceptions import HTTP_EXCEPTIONS, NsqHttpException
from ..utils import _convert_to_str


class NsqHTTPConnection:
    """XXX"""

    def __init__(self, host='127.0.0.1', port=4150, *, loop):
        self._loop = loop
        self._endpoint = (host, port)
        self._base_url = 'http://{0}:{1}/'.format(*self._endpoint)

        self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(),
                                              loop=self._loop)

    @property
    def endpoint(self):
        return 'http://{0}:{1}'.format(*self._endpoint)

    async def close(self):
        return await self._session.close()

    async def perform_request(self, method, url, params, body):
        _body = _convert_to_str(body) if body else body
        url = self._base_url + url
        print(method, url, params, _body)
        try:
            resp = await self._session.request(method, url,
                                               params=params,
                                               data=_body)
        except Exception as tmp:
            print('exception', tmp)
        resp_body = await resp.text()
        try:
            response = json.loads(resp_body)
        except ValueError:
            return resp_body

        if not (200 <= resp.status <= 300):
            extra = None
            try:
                extra = json.loads(resp_body)
            except ValueError:
                pass
            exc_class = HTTP_EXCEPTIONS.get(resp.status, NsqHttpException)
            raise exc_class(resp.status, resp_body, extra)
        return response

    def __repr__(self):
        cls_name = self.__class__.__name__
        return '<{}: {}>'.format(cls_name, self._endpoint)
