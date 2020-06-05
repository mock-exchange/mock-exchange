from __future__ import absolute_import, division, print_function
import datetime as dt

from six.moves import range
from typing import Text

__all__ = [
    'truncate',
    'truncate_second',
    'truncate_minute',
    'truncate_nth_minute',
    'truncate_hour',
    'truncate_nth_hour',
    'truncate_day',
    'truncate_week',
    'truncate_month',
    'truncate_quarter',
    'truncate_half_year',
    'truncate_year',
]

PERIODS = {
    'second': dict(microsecond=0),
    'minute': dict(microsecond=0, second=0),
    'hour': dict(microsecond=0, second=0, minute=0),
    'day': dict(microsecond=0, second=0, minute=0, hour=0,),
    'month': dict(microsecond=0, second=0, minute=0, hour=0, day=1),
    'year': dict(microsecond=0, second=0, minute=0, hour=0, day=1, month=1),
}
ODD_PERIODS = ['week', 'quarter', 'half_year']


def truncate_second(datetime):
    # type: (dt.datetime) -> dt.datetime
    ''' Sugar for :py:func:`truncate(datetime, 'second')` '''
    return truncate(datetime, 'second')


def truncate_minute(datetime):
    # type: (dt.datetime) -> dt.datetime
    ''' Sugar for :py:func:`truncate(datetime, 'minute')` '''
    return truncate(datetime, 'minute')


def truncate_hour(datetime):
    # type: (dt.datetime) -> dt.datetime
    ''' Sugar for :py:func:`truncate(datetime, 'hour')` '''
    return truncate(datetime, 'hour')


def truncate_day(datetime):
    # type: (dt.datetime) -> dt.datetime
    ''' Sugar for :py:func:`truncate(datetime, 'day')` '''
    return truncate(datetime, 'day')


def truncate_week(datetime):
    # type: (dt.datetime) -> dt.datetime
    '''
    Truncates a date to the first day of an ISO 8601 week, i.e. monday.

    :params datetime: an initialized datetime object
    :return: `datetime` with the original day set to monday
    :rtype: :py:mod:`datetime` datetime object
    '''
    datetime = truncate(datetime, 'day')
    return datetime - dt.timedelta(days=datetime.isoweekday() - 1)


def truncate_month(datetime):
    # type: (dt.datetime) -> dt.datetime
    ''' Sugar for :py:func:`truncate(datetime, 'month')` '''
    return truncate(datetime, 'month')


def truncate_quarter(datetime):
    # type: (dt.datetime) -> dt.datetime
    '''
    Truncates the datetime to the first day of the quarter for this date.

    :params datetime: an initialized datetime object
    :return: `datetime` with the month set to the first month of this quarter
    :rtype: :py:mod:`datetime` datetime object
    '''
    datetime = truncate(datetime, 'month')

    month = datetime.month
    if 1 <= month <= 3:
        return datetime.replace(month=1)
    elif 4 <= month <= 6:
        return datetime.replace(month=4)
    elif 7 <= month <= 9:
        return datetime.replace(month=7)
    elif 10 <= month <= 12:
        return datetime.replace(month=10)


def truncate_half_year(datetime):
    # type: (dt.datetime) -> dt.datetime
    '''
    Truncates the datetime to the first day of the half year for this date.

    :params datetime: an initialized datetime object
    :return: `datetime` with the month set to the first month of this half year
    :rtype: :py:mod:`datetime` datetime object
    '''
    datetime = truncate(datetime, 'month')

    month = datetime.month

    if 1 <= month <= 6:
        return datetime.replace(month=1)
    elif 7 <= month <= 12:
        return datetime.replace(month=7)


def truncate_year(datetime):
    # type: (dt.datetime) -> dt.datetime
    ''' Sugar for :py:func:`truncate(datetime, 'year')` '''
    return truncate(datetime, 'year')


def truncate_nth_minute(datetime, nth_minute):
    # type: (dt.datetime, int) -> dt.datetime
    '''
    Truncates the datetime to the nth minute closest to it. For instance
    with 5 as the argument it becomes the nearest five minute down from
    the datetime.

    :param datetime: an initialized datetime object
    :type datetime: datetime
    :param nth_minute: the minute to truncate to
    :type nth_minute: int
    :rtype: :py:mod:`datetime` datetime object
    '''
    if not 0 <= nth_minute < 60:
        raise ValueError(
            '`nth_minute` must be >= 0 and < 60, was {0}'.format(nth_minute)
        )

    for m in range(0, 60, nth_minute):
        if m <= datetime.minute < m + nth_minute:
            return datetime.replace(minute=m, second=0, microsecond=0)


def truncate_nth_hour(datetime, nth_hour):
    # type: (dt.datetime, int) -> dt.datetime
    '''
    Truncates the datetime to the nth hour closest to it. For instance
    with 5 as the argument it becomes the nearest five hour down from
    the datetime.

    :param datetime: an initialized datetime object
    :type datetime: datetime
    :param nth_hour: the hour to truncate to
    :type nth_hour: int
    :rtype: :py:mod:`datetime` datetime object
    '''
    if not 0 <= nth_hour < 24:
        raise ValueError(
            '`nth_hour` must be >= 0 and < 24, was {0}'.format(nth_hour)
        )

    for h in range(0, 24, nth_hour):
        if h <= datetime.hour < h + nth_hour:
            return datetime.replace(hour=h, minute=0, second=0, microsecond=0)


def truncate(datetime, truncate_to='day'):
    # type: (dt.datetime, Text) -> dt.datetime
    '''
    Truncates a datetime to have the values with higher precision than
    the one set as `truncate_to` as zero (or one for day and month).

    Possible values for `truncate_to`:

    * second
    * minute
    * <num>_minute (i.e. 5_minute, 19_minute, 2_minute, etc.)
    * hour
    * day
    * week (iso week i.e. to monday)
    * month
    * quarter
    * half_year
    * year

    Examples::

       >>> truncate(dt.datetime(2012, 12, 12, 12), 'day')
       datetime(2012, 12, 12)
       >>> truncate(dt.datetime(2012, 12, 14, 12, 15), 'quarter')
       datetime(2012, 10, 1)
       >>> truncate(dt.datetime(2012, 3, 1), 'week')
       datetime(2012, 2, 27)

    :params datetime: an initialized datetime object
    :params truncate_to: The highest precision to keep its original data.
    :return: datetime with `truncated_to` as the highest level of precision
    :rtype: :py:mod:`datetime` datetime object
    '''
    if truncate_to in PERIODS:
        return datetime.replace(**PERIODS[truncate_to])
    elif truncate_to in ODD_PERIODS:
        if truncate_to == 'week':
            return truncate_week(datetime)
        elif truncate_to == 'quarter':
            return truncate_quarter(datetime)
        elif truncate_to == 'half_year':
            return truncate_half_year(datetime)
    elif truncate_to.endswith('_minute'):
        return truncate_nth_minute(datetime, int(truncate_to.split('_')[0]))
    elif truncate_to.endswith('_hour'):
        return truncate_nth_hour(datetime, int(truncate_to.split('_')[0]))
    else:
        raise ValueError('truncate_to not valid. Valid periods: {}'.format(
            ', '.join(list(PERIODS.keys()) + ODD_PERIODS)
        ))
