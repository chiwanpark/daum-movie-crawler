# -*- coding: utf-8 -*-
import aiohttp
import asyncio
import asynctest

from crawler.proxy import ProxyMixin
from crawler.logging import LoggingMixin


class ProxyMixinTest(asynctest.TestCase, LoggingMixin):
    async def test_load_proxy_list(self):
        mixin = ProxyMixin()
        self.assertTrue(await mixin.update_proxy())

    async def test_valid_proxies(self):
        mixin = ProxyMixin()
        self.assertTrue(await mixin.update_proxy())

        success = 0
        async with aiohttp.ClientSession() as sess:
            for i in range(1, 11):
                proxy = mixin.pick_proxy()
                self.logger.info('test %d, proxy = %s', i, proxy)
                try:
                    async with sess.get('http://icanhazip.com', proxy=proxy) as resp:
                        if resp.status == 200:
                            success += 1
                            self.logger.info("connection test succeeded")
                        else:
                            self.logger.info("connection test failed (status = %d)", resp.status)
                except aiohttp.ClientConnectionError:
                    self.logger.info("connection test failed (proxy connection error)")
                except asyncio.TimeoutError:
                    self.logger.info("connection test failed (timeout)")

        self.assertGreaterEqual(success, 1)

