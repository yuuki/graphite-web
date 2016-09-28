# -*- coding: utf-8 -*-
"""Copyright 2008 Orbitz WorldWide

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

from graphite.logger import log
from graphite.storage import STORE
from graphite.readers import FetchInProgress
from django.conf import settings
from graphite.util import epoch
from graphite.dynamodb import DynamoDB

from traceback import format_exc

class TimeSeries(list):
  def __init__(self, name, start, end, step, values, consolidate='average'):
    list.__init__(self, values)
    self.name = name
    self.start = start
    self.end = end
    self.step = step
    self.consolidationFunc = consolidate
    self.valuesPerPoint = 1
    self.options = {}


  def __eq__(self, other):
    if isinstance(other, TimeSeries):
      color_check = True
      if hasattr(self, 'color'):
        if hasattr(other, 'color'):
          color_check = (self.color == other.color)
        else:
          color_check = False
      elif hasattr(other, 'color'):
        color_check = False

      return ((self.name, self.start, self.end, self.step, self.consolidationFunc, self.valuesPerPoint, self.options) ==
              (other.name, other.start, other.end, other.step, other.consolidationFunc, other.valuesPerPoint, other.options)) and list.__eq__(self, other) and color_check
    return False


  def __iter__(self):
    if self.valuesPerPoint > 1:
      return self.__consolidatingGenerator( list.__iter__(self) )
    else:
      return list.__iter__(self)


  def consolidate(self, valuesPerPoint):
    self.valuesPerPoint = int(valuesPerPoint)


  def __consolidatingGenerator(self, gen):
    buf = []
    for x in gen:
      buf.append(x)
      if len(buf) == self.valuesPerPoint:
        while None in buf: buf.remove(None)
        if buf:
          yield self.__consolidate(buf)
          buf = []
        else:
          yield None
    while None in buf: buf.remove(None)
    if buf: yield self.__consolidate(buf)
    else: yield None
    raise StopIteration


  def __consolidate(self, values):
    usable = [v for v in values if v is not None]
    if not usable: return None
    if self.consolidationFunc == 'sum':
      return sum(usable)
    if self.consolidationFunc == 'average':
      return float(sum(usable)) / len(usable)
    if self.consolidationFunc == 'max':
      return max(usable)
    if self.consolidationFunc == 'min':
      return min(usable)
    raise Exception("Invalid consolidation function: '%s'" % self.consolidationFunc)


  def __repr__(self):
    return 'TimeSeries(name=%s, start=%s, end=%s, step=%s)' % (self.name, self.start, self.end, self.step)


  def getInfo(self):
    """Pickle-friendly representation of the series"""
    return {
      'name' : self.name,
      'start' : self.start,
      'end' : self.end,
      'step' : self.step,
      'values' : list(self),
    }


# Data retrieval API
def fetchData(requestContext, pathExpr):
  seriesList = {}
  startTime = int( epoch( requestContext['startTime'] ) )
  endTime   = int( epoch( requestContext['endTime'] ) )

  def _fetchData(pathExpr, startTime, endTime, seriesList):
    dyna = DynamoDB()
    values = dyna.fetch(pathExpr, startTime, endTime)
    # とりあえずstepは60秒決め打ちで
    series = TimeSeries(pathExpr, startTime, endTime, 60, values)
    series.pathExpression = pathExpr
    seriesList[series.name] = series

    # Stabilize the order of the results by ordering the resulting series by name.
    # This returns the result ordering to the behavior observed pre PR#1010.
    return [seriesList[k] for k in sorted(seriesList)]

  retries = 1 # start counting at one to make log output and settings more readable
  while True:
    try:
      seriesList = _fetchData(pathExpr, startTime, endTime, seriesList)
      return seriesList
    except Exception, e:
      if retries >= settings.MAX_FETCH_RETRIES:
        log.exception("Failed after %s retry! Root cause:\n%s" %
            (settings.MAX_FETCH_RETRIES, format_exc()))
        raise e
      else:
        log.exception("Got an exception when fetching data! Try: %i of %i. Root cause:\n%s" %
                     (retries, settings.MAX_FETCH_RETRIES, format_exc()))
        retries += 1


def nonempty(series):
  for value in series:
    if value is not None:
      return True

  return False
