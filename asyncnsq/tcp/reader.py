import asyncio
import random
import logging
import time
from asyncnsq.http import NsqLookupd
from asyncnsq.tcp.reader_rdy import RdyControl
from functools import partial

from . import consts
from .connection import create_connection
from .consts import SUB
from ..utils import retry_iterator

logger = logging.getLogger(__package__)


async def create_reader(nsqd_tcp_addresses=None, loop=None,
                        max_in_flight=42, lookupd_http_addresses=None):
    """"
    initial function to get consumer
    param: nsqd_tcp_addresses: tcp addrs with no protocol.
        such as ['127.0.0.1:4150','182.168.1.1:4150']
    param: max_in_flight: number of messages get but not finish or req
    param: lookupd_http_addresses: first priority.if provided nsqd will neglected
    """
    loop = loop or asyncio.get_event_loop()
    if lookupd_http_addresses:
        reader = Reader(lookupd_http_addresses=lookupd_http_addresses,
                        max_in_flight=max_in_flight, loop=loop)
    else:
        if nsqd_tcp_addresses is None:
            nsqd_tcp_addresses = ['127.0.0.1:4150']
        nsqd_tcp_addresses = [i.split(':') for i in nsqd_tcp_addresses]
        reader = Reader(nsqd_tcp_addresses=nsqd_tcp_addresses,
                        max_in_flight=max_in_flight, loop=loop)
    await reader.connect()
    return reader


class Reader:
    """
    NSQ tcp reader
    """

    def __init__(self, nsqd_tcp_addresses=None, lookupd_http_addresses=None,
                 max_in_flight=42, loop=None, heartbeat_interval=30000,
                 feature_negotiation=True,
                 tls_v1=False, snappy=False, deflate=False, deflate_level=6,
                 sample_rate=0, consumer=False, log_level=None):
        self._config = {
            "deflate": deflate,
            "deflate_level": deflate_level,
            "sample_rate": sample_rate,
            "snappy": snappy,
            "tls_v1": tls_v1,
            "heartbeat_interval": heartbeat_interval,
            'feature_negotiation': feature_negotiation,
        }
        self._nsqd_tcp_addresses = nsqd_tcp_addresses or []
        self._lookupd_http_addresses = lookupd_http_addresses or []

        self._max_in_flight = max_in_flight
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue(loop=self._loop)
        self._redistribute_task = None
        self._reconnect_task = None

        self._connections = {}

        self._idle_timeout = 10

        self._rdy_control = None
        self._max_in_flight = max_in_flight

        self._status = consts.INIT

        self._is_subscribe = False
        self._redistribute_timeout = 5  # sec
        self._lookupd_poll_time = 30  # sec
        self.topic = None
        self.channel = None
        self._rdy_control = RdyControl(idle_timeout=self._idle_timeout,
                                       max_in_flight=self._max_in_flight,
                                       loop=self._loop)

    async def connect(self):
        logging.info('reader connecting')
        if self._lookupd_http_addresses:
            """
            because lookupd must find a topic to find nsqds
            so, lookupd connect changed in to subscribe func
            """
            pass
        if self._nsqd_tcp_addresses:
            for host, port in self._nsqd_tcp_addresses:
                conn = await create_connection(
                    host, port, queue=self._queue,
                    loop=self._loop)
                await self.prepare_conn(conn)
            self._connections[conn.id] = conn
            self._rdy_control.add_connections(self._connections)
            self._status = consts.CONNECTED
        self._loop.create_task(self.auto_reconnect())

    async def prepare_conn(self, conn):
        conn.rdy_state = 2
        conn._on_message = partial(self._on_message, conn)
        result = await conn.identify(**self._config)

    def _on_message(self, conn, msg):
        # should not be coroutine
        # update connections rdy state
        conn.rdy_state = int(conn.rdy_state) - 1

        conn._last_message = time.time()
        if conn._on_rdy_changed_cb is not None:
            conn._on_rdy_changed_cb(conn.id)
        return msg

    async def _poll_lookupd(self, host, port):
        nsqlookup_conn = NsqLookupd(host, port, loop=self._loop)
        try:
            res = await nsqlookup_conn.lookup(self.topic)
            logger.info('lookupd response')
            logger.info(res)
        except Exception as tmp:
            logger.error(tmp)
            logger.exception(tmp)

        for producer in res['producers']:
            host = producer['broadcast_address']
            port = producer['tcp_port']
            tmp_id = "tcp://{}:{}".format(host, port)
            if tmp_id not in self._connections:
                logger.debug(('host, port', host, port))
                conn = await create_connection(
                    host, port, queue=self._queue,
                    loop=self._loop)
                logger.debug(('conn.id:', conn.id))
                self._connections[conn.id] = conn
                self._rdy_control.add_connection(conn)
        await nsqlookup_conn.close()

    async def subscribe(self, topic, channel):
        self.topic = topic
        self.channel = channel
        self._is_subscribe = True

        if self._lookupd_http_addresses:
            await self._lookupd()
        for conn in self._connections.values():
            await self.sub(conn, topic, channel)
            if conn._on_rdy_changed_cb is not None:
                conn._on_rdy_changed_cb(conn.id)
        if not self._redistribute_task:
            self._redistribute_task = self._loop.create_task(
                self._redistribute()
            )

    async def sub(self, conn, topic, channel):
        await conn.execute(SUB, topic, channel)

    def wait_messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subscribe:
            fut = self._loop.create_task(self._queue.get())
            yield fut

    async def messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subscribe:
            result = await self._queue.get()
            yield result

    async def reconnect(self, conn):
        logger.debug(f'reader reconnect {conn.id}')

        if not conn.closed:
            return

        conn = await create_connection(
            conn._host, conn._port, queue=self._queue, loop=self._loop)
        await self.prepare_conn(conn)

        logger.info(f'Connection {conn.id} established')
        self._connections[conn.id] = conn
        self._rdy_control.add_connections(self._connections)

        await self.subscribe(self.topic, self.channel)
        # TODO: _cmd_queue is not working here...
        await self._rdy_control._update_rdy(conn.id)

    async def auto_reconnect(self):
        logger.debug('reader autoreconnect')
        timeout_generator = retry_iterator(init_delay=0.1, max_delay=10.0)
        while True:
            logger.debug('autoreconnect check loop')

            if self._status != consts.RECONNECTING:
                for conn in list(self._connections.values()).copy():
                    if conn.closed:
                        logger.info('Reconnect reader {}'.format(conn.id))
                        self._status = consts.RECONNECTING

                        try:
                            await self.reconnect(conn)
                        except ConnectionError:
                            logger.error(f'Can not connect to: {conn.id}')
                self._status = consts.CONNECTED

            t = next(timeout_generator)
            await asyncio.sleep(t, loop=self._loop)

    def is_starved(self):
        conns = self._connections.values()
        return any(conn.is_starved() for conn in conns)

    async def _redistribute(self):
        while self._is_subscribe:
            self._rdy_control.redistribute()
            await asyncio.sleep(self._redistribute_timeout,
                                loop=self._loop)

    async def _lookupd(self):
        host, port = random.choice(self._lookupd_http_addresses)
        await self._poll_lookupd(host, port)

    def stop(self):
        self._is_subscribe = False
        if self._redistribute_task:
            self._redistribute_task.cancel()
        self._reconnect_task.cancel()
        for connection in self._connections:
            connection.close()
        self._loop.run_until_complete(self._rdy_control.stop())
