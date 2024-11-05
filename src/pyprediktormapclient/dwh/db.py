import logging

from pyprediktorutilities.dwh.dwh import Dwh

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Db(Dwh):
    """This class is not used in the current version of pyPrediktorMapClient.

    It was replaced by Dwh from pyPrediktorUtilities. It is left here
    for backwards compatibility only.
    """
