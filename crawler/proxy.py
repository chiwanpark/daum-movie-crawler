# -*- coding: utf-8 -*-
import re
import random

import aiohttp

from crawler.logging import LoggingMixin


class ProxyMixin(LoggingMixin):
    @property
    def _proxy_list_url(self):
        return 'http://spys.me/proxy.txt'

    @property
    def _proxy_pattern(self):
        if not hasattr(self, '__proxy_pattern') or not self.__proxy_pattern:
            self.__proxy_pattern = re.compile(
                '(?P<addr>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}:[0-9]{1,5}) (?P<option>.+)')
        
        return self.__proxy_pattern

    def _parse_proxy_line(self, line):
        match = self._proxy_pattern.match(line)
        if not match:
            return None

        option = match.group('option').strip()
        return {
            'addr': match.group('addr').strip(),
            'country': option[:2],
            'anonymity': option[3],
            'support_https': '-S' in option[4:],
            'google_passed': '+' == option[-1]
        }

    async def _fetch_proxy_list(self):
        self.logger.info('retrieve proxy list from %s', self._proxy_list_url)
        async with aiohttp.ClientSession() as sess:
            async with sess.get(self._proxy_list_url) as resp:
                if resp.status != 200:
                    self.logger.warn('failed to retrieve proxy list! status code = %d', resp.status)
                    return None
                content = await resp.text()
                self.logger.info('proxy list downloaded')
                return content

    async def update_proxy(self):
        data = await self._fetch_proxy_list()
        if not data:
            return False

        self._proxies = []
        for line in data.strip().split('\n'):
            entry = self._parse_proxy_line(line)
            if not entry:
                continue
            self._proxies.append(entry)

        return len(self._proxies) > 0

    def pick_proxy(self, google_passed=True, support_https=True):
        proxy = random.choice(self._proxies)
        while (google_passed and not proxy['google_passed']) or \
              (support_https and not proxy['support_https']):
            proxy = random.choice(self._proxies)

        return f'http://{proxy["addr"]}'
