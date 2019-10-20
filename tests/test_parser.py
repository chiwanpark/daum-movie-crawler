# -*- coding: utf-8 -*-
import os

import aiofiles
import asynctest

from crawler.parser import Parser
from crawler.logging import LoggingMixin


class ParserTest(asynctest.TestCase, LoggingMixin):
    async def _load_file(self, name):
        path = os.path.join(os.path.dirname(__file__), 'res', name)
        async with aiofiles.open(path, 'r') as f:
            content = await f.read()

        return content

    async def _load_movie_main(self):
        return await self._load_file('movie_main.html')

    async def _load_movie_reviews(self):
        return await self._load_file('movie_reviews.html')

    async def _load_movie_user_reviews(self):
        return await self._load_file('movie_user_reviews.html')

    async def test_get_links(self):
        content = await self._load_movie_main()
        parser = Parser(content)
        links = parser.get_links()

        self.logger.info('# of links: %d', len(links))

        self.assertTrue(any(('movieId=128635' in link for link in links)))
        self.assertTrue(any(('personId=271829' in link for link in links)))
        self.assertTrue(any(('personId=518464' in link for link in links)))

    async def test_get_metadata(self):
        content = await self._load_movie_main()
        parser = Parser(content)
        metadata = parser.get_metadata()

        self.assertTrue(metadata is not None)
        self.assertEqual('우리집 (2019)', metadata['title'])
        self.assertEqual('드라마/가족', metadata['genre'])
        self.assertEqual('한국', metadata['country'])
        self.assertEqual(92, metadata['running_time'])

    async def test_get_ratings_per_movie_1(self):
        content = await self._load_movie_main()
        parser = Parser(content)
        ratings = parser.get_ratings_per_movie()

        answer = [
            ('ckh5SQ==', 0),
            ('Q0s1Yk0=', 9),
            ('OHVFYTQ=', 10),
            ('NGsxa0M=', 10),
            ('NHJ5aHM=', 10)
        ]
        self.assertEqual(answer, ratings)

    async def test_get_ratings_per_movie_2(self):
        content = await self._load_movie_reviews()
        parser = Parser(content)
        ratings = parser.get_ratings_per_movie()

        answer = [
            ('OEhrRm4=', 9),
            ('OEVQQnU=', 1),
            ('MkpxV2Y=', 1),
            ('QmtPMWI=', 9),
            ('VU9ROA==', 7),
            ('dEdabQ==', 10),
            ('NFB0NUY=', 6),
            ('M0tpUlA=', 10),
            ('QzF4Wnc=', 10),
            ('OUMwUnA=', 7)
        ]
        self.assertEqual(answer, ratings)

    async def test_get_ratings_per_user(self):
        content = await self._load_movie_user_reviews()
        parser = Parser(content)
        ratings = parser.get_ratings_per_user()

        answer = [
            ('127878', 10),
            ('78539', 10),
            ('42238', 1),
            ('70404', 9)
        ]
        self.assertEqual(answer, ratings)
