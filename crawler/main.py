# -*- coding: utf-8 -*-
import os
import random
import asyncio
from collections import deque

import aiohttp
import aiofiles
from fire import Fire

from crawler.parser import Parser
from crawler.proxy import ProxyMixin


class Main(ProxyMixin):
    def __init__(self):
        self._tasks = deque()
        self._visited = set()
        self._uas = [
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
            'Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0',
            'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:63.0) Gecko/20100101 Firefox/63.0',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.13; ko; rv:1.9.1b2) Gecko/20081201 Firefox/60.0'
        ]
        self._count = 0
        self._ratings = []
        self._metadata = []

    async def _fetch_content(self, url, sess):
        proxy = self.pick_proxy() if self._use_proxy else None
        headers = {
            'User-Agent': random.choice(self._uas)
        }
        try:
            async with sess.get(url, proxy=proxy, headers=headers, proxy_headers=headers) as resp:
                if resp.status != 200:
                    return url, None
                return url, await resp.read()
        except aiohttp.ClientConnectionError:
            self.logger.info('connection failed (client connection error)')
        except aiohttp.ClientHttpProxyError:
            self.logger.info('connection failed (client http proxy error')
        except asyncio.TimeoutError:
            self.logger.info('connection failed (timeout)')
        self.remove_proxy(proxy)
        return url, None

    async def _fetch_contents(self, urls):
        results = {}
        async with aiohttp.ClientSession() as sess:
            futs = [asyncio.ensure_future(self._fetch_content(url, sess)) for url in urls]
            results = dict(await asyncio.gather(*futs))

        if not self._use_proxy:
            sleep_secs = random.randint(0, 10)
            self.logger.info(f'sleep {sleep_secs} secs...')
            await asyncio.sleep(sleep_secs)

        return results

    def _parse_ratings_per_user(self, url, parser):
        uid = parser.get_user_id(url)
        if not uid:
            return []

        ratings = [(uid, iid, rating) for iid, rating in parser.get_ratings_per_user()]
        self.logger.info(f'parsed {len(ratings)} ratings')
        return ratings

    def _parse_ratings_per_movie(self, url, parser):
        iid = parser.get_movie_id(url)
        if not iid:
            return []
       
        ratings = [(uid, iid, rating) for uid, rating in parser.get_ratings_per_movie()]
        self.logger.info(f'parsed {len(ratings)} ratings')
        return ratings

    def _parse_metadata(self, url, parser):
        iid = parser.get_movie_id(url)
        if not iid:
            return None
        metadata = parser.get_metadata()
        metadata['movieid'] = iid

        return metadata

    def _parse_content(self, url, content):
        if url in self._visited:
            return
        self._visited.add(url)
        self.logger.info(f'url {url} visited')

        parser = Parser(content)
        links = parser.get_links() - self._visited
        self.logger.info(f'{len(links)} links added')
        self._tasks.extend(links)

        if 'other/moviePoint' in url:
            ratings = self._parse_ratings_per_user(url, parser)
            if ratings:
                self._ratings.extend(ratings)
        elif 'moviedb/main' in url:
            metadata = self._parse_metadata(url, parser)
            if metadata:
                self._metadata.append(metadata)
            ratings = self._parse_ratings_per_movie(url, parser)
            if ratings:
                self._ratings.extend(ratings)
        elif 'moviedb/grade' in url:
            ratings = self._parse_ratings_per_movie(url, parser)
            if ratings:
                self._ratings.extend(ratings)

    async def _flush_data(self):
        if len(self._ratings) < 100:
            return

        metadata_path = os.path.join(self._output_path, 'metadata.csv')
        ratings_path = os.path.join(self._output_path, 'ratings.csv')
        async with aiofiles.open(metadata_path, 'a') as f_metadata, \
                   aiofiles.open(ratings_path, 'a') as f_ratings:
            metadata = (f'{m["movieid"]},"{m["title"]}","{m["genre"]}","{m["country"]}",{m["running_time"]}' for m in self._metadata)
            ratings = (f'{r[0]},{r[1]},{r[2]}' for r in self._ratings)
            fut1 = asyncio.ensure_future(f_metadata.write('\n'.join(metadata) + '\n'))
            fut2 = asyncio.ensure_future(f_ratings.write('\n'.join(ratings) + '\n'))

            await asyncio.gather(fut1, fut2)
            self.logger.info(f'{len(self._ratings)} ratings and {len(self._metadata)} movies flushed')

        self._ratings = []
        self._metadata = []

    async def _run_main(self):
        self.logger.info('update proxies...')
        await self.update_proxy()

        while len(self._tasks) > 0:
            batch = []
            for _ in range(min(len(self._tasks), self._sz_batch)):
                batch.append(self._tasks.popleft())
            self.logger.info(f'len(batch): {len(batch)}, len(self._tasks): {len(self._tasks)}')

            results = await self._fetch_contents(batch)
            for url, result in results.items():
                if result is None:
                    self._tasks.append(url)
                else:
                    self._parse_content(url, result)

            await self._flush_data()

    def run(self, seed, output_path='./res', use_proxy=True, sz_batch=10):
        self._tasks.append(seed)
        self._use_proxy = use_proxy
        self._sz_batch = sz_batch
        self._output_path = output_path

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._run_main())
        loop.close()
