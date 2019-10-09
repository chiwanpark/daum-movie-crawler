# -*- coding: utf-8 -*-
import logging


class LoggingMixin(object):
    @property
    def logger(self):
        if not hasattr(self, '_logger') or not self._logger:
            self._logger = logging.getLogger(self.__class__.__name__)
            self._logger.setLevel(logging.INFO)

            formatter = logging.Formatter(
                '[%(levelname)-8s] %(asctime)s [%(filename)s] [%(funcName)s:%(lineno)d] ' +
                '%(message)s', '%Y-%m-%d %H:%M:%S')

            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            handler.setFormatter(formatter)

            self._logger.addHandler(handler)

        return self._logger
