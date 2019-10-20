# -*- coding: utf-8 -*-
import re

from bs4 import BeautifulSoup


_valid_link_ptn = [
    re.compile(r'movieId=([0-9]+)'),
    re.compile(r'personId=([0-9]+)'),
    re.compile(r'uid=([^&]+)'),
    re.compile(r'productionId=([0-9]+)')
]
_valid_runtime_ptn = re.compile(r'([0-9]+)분')
_space_ptn = re.compile(r'[\t]+|\s{2,}')


class Parser(object):
    def __init__(self, content):
        if isinstance(content, bytes):
            content = content.decode('utf-8')
        self._soup = BeautifulSoup(content, 'html.parser')

    def _matched_token(self, string, ptn):
        m = ptn.search(string)
        if m:
            m = m.group(1)
        return m

    def get_movie_id(self, url):
        return self._matched_token(url, _valid_link_ptn[0])

    def get_user_id(self, url):
        return self._matched_token(url, _valid_link_ptn[2])

    def get_links(self):
        links = set()

        for tag in self._soup.find_all('a'):
            href = tag.get('href')
            if not href or 'reservation' in href:
                continue
            for ptn in _valid_link_ptn:
                if ptn.search(href):
                    links.add('https://movie.daum.net' + href)
                    break

        return links

    def get_metadata(self):
        summary = self._soup.find_all('div', 'movie_summary')
        if not summary:
            return None
        try:
            title = summary[0].find('strong', 'tit_movie').string.strip()
            meta_list = summary[0].find('dl', 'list_movie')
            dds = meta_list.find_all('dd')
        except (IndexError, ValueError, AttributeError) as e:
            return None
        try:
            genre = dds[0].string.strip()
        except (IndexError, ValueError, AttributeError) as e:
            genre = 'N/A'
        try:
            country = dds[1].string.strip()
        except (IndexError, ValueError, AttributeError) as e:
            country = 'N/A'
        try:
            running_time = dds[3].string
            if running_time:
                running_time = _valid_runtime_ptn.search(running_time.strip())
                if running_time:
                    running_time = int(running_time.group(1))
                else:
                    running_time = -1
            else:
                running_time = -1
        except (IndexError, ValueError, AttributeError) as e:
            running_time = -1

        return {
            'title': _space_ptn.sub('', title),
            'genre': _space_ptn.sub('', genre),
            'country': _space_ptn.sub('', country),
            'running_time': running_time
        }

    def get_ratings_per_movie(self):
        ratings = []
        reviews = self._soup.find_all('div', 'review_info')
        for review in reviews:
            uid = review.find('a', '#grade')
            if not uid:
                uid = review.find('strong', '#grade')
                if not uid:
                    continue
                uid = uid.find('a')
            uid = uid.get('href')
            uid = _valid_link_ptn[2].search(uid)
            if not uid:
                continue
            uid = uid.group(1)
            rating = review.find('em', 'emph_grade').string
            if not rating:
                continue
            rating = int(rating)

            ratings.append((uid, rating))

        return ratings

    def get_ratings_per_user(self):
        ratings = []
        reviews = self._soup.find_all('div', 'rate_info')
        for review in reviews:
            iid = review.find('a', 'tit_subject')
            if not iid:
                continue
            iid = iid.get('name')
            rating = review.find('em', 'emph_rate').string
            if not rating:
                continue
            rating = int(rating)

            ratings.append((iid, rating))

        return ratings
