import math

__author__ = 'jawaad'


def chunks(l, n):
    """ Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def rnd(x1):
    """
    Rounds and integer-izes a number.
    :param x1:
    :type x1:
    :return:
    :rtype:
    """
    return int(math.ceil(x1))