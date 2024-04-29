.. _parameter_value_format:


**********************
Parameter value format
**********************

.. note::

   Client code should almost never convert parameter values to JSON and back manually.
   For most cases, JSON should be considered an implementation detail.
   Clients should rather use :func:`.to_database` and :func:`.from_database` which shield
   from abrupt changes in the database representation.

Parameter values are specified using JSON in the ``value`` field of the ``parameter_value`` table.
This document describes the JSON specification for parameter values of special type
(namely, date-time, duration, time-pattern, time-series, array, and map.)

A value of special type is a JSON object with two mandatory properties, ``type`` and ``data``:

- ``type`` indicates the value *type* and must be a JSON string
  (either ``date_time``, ``duration``, ``dictionary``, ``time_pattern``, ``time_series``, ``array``, or ``map``).
- ``data`` specifies the value *itself* and must be a JSON object in accordance with ``type`` as explained below.

Date-time
---------

If the ``type`` property is ``date_time``, then the ``data`` property specifies a date/time
and must be a JSON string in the `ISO8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format.

Example
~~~~~~~

.. code-block:: json

   {
     "type": "date_time",
     "data": "2019-06-01T22:15:00+01:00"
   }


Duration
--------

If the  ``type`` property is ``duration``, then the ``data`` property specifies an extension of time
where the accepted values are the following:

- The number of time-units, specified as a 'verbose' JSON string.
  The format is ``x unit``, where ``x`` is an integer
  and ``unit`` is either ``year``, ``month``, ``day``, ``hour``, ``minute``, or ``second``
  (either singular or plural).
- The number of time-units, specified as a 'compact' JSON string.
  The format is ``xU``, where ``x`` is an integer
  and ``U`` is either ``Y`` (for year), ``M`` (for month), ``D`` (for day),
  ``h`` (for hour), ``m`` (for minute), or ``s`` (for second).
- The number of *minutes*, specified as a JSON integer.

.. note::

   The array version of Duration is deprecated and no longer supported.
   Use the Array type for variable durations.

Examples
~~~~~~~~

Verbose string:

.. code-block:: json

   {
     "type": "duration",
     "data": "1 hour"
   }

Compact string:

.. code-block:: json

   {
     "type": "duration",
     "data": "1h"
   }

Integer:

.. code-block:: json

   {
     "type": "duration",
     "data": 60
   }

Time-pattern
------------

If the ``type`` property is ``time_pattern``, then the ``data`` property specifies *time-patterned data*.
This is data that varies *periodically* in time taking specific *values* at specific *time-periods* (such as summer and winter).
Values must be JSON numbers, whereas time-periods must be JSON strings
where the accepted values are the following:

- An interval of time in a given time-unit.
  The format is ``Ua-b``, where ``U`` is either ``Y`` (for year), ``M`` (for month), ``D`` (for day), ``WD`` (for weekday),
  ``h`` (for hour), ``m`` (for minute), or ``s`` (for second);
  and ``a`` and ``b`` are two integers corresponding to the lower and upper bound, respectively.
- An intersection of intervals.
  The format is ``s1;s2;...``,
  where ``s1``, ``s2``, ..., are intervals as described above.
- A union of ranges.
  The format is ``r1,r2,...``,
  where ``r1``, ``r2``, ..., are either intervals or intersections of intervals as described above.

The ``data`` property must be a JSON object mapping time periods to values.

A time-pattern may have an additional property, ``index_name``.
``index_name`` must be a JSON string. If not specified, a default name 'p' will be used.

Example
~~~~~~~

The following corresponds to a parameter which takes the value ``300`` in months 1 to 4 *and* 9 to 12,
and the value ``221.5`` in months 5 to 8.

.. code-block:: json

   {
     "type": "time_pattern",
     "data": {
       "M1-4,M9-12": 300,
       "M5-8": 221.5
     }
   }

Time-series
-----------

If the ``type`` property is ``time_series``, then the ``data`` property specifies time-series data.
This is data that varies *arbitrarily* in time taking specific *values* at specific *time-stamps*.
Values must be JSON numbers,
whereas time-stamps must be JSON strings in the `ISO8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format.

Accepted values for the ``data`` property are the following:

- A JSON object mapping time-stamps to values.
- A two-column JSON array listing tuples of the form [time-stamp, value].
- A (one-column) JSON array of values.
  In this case it is assumed that the time-series begins at the first hour of *any* year,
  has a resolution of one hour, and repeats cyclically until the *end* of time.

In case of time-series, the specification may have two additional properties, ``index`` and ``index_name``.
``index`` must be a JSON object with the following properties, all of them optional:

- ``start``: the *first* time-stamp, used in case ``data`` is a one-column array (ignored otherwise).
  It must be a JSON string in the `ISO8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format.
  The default is ``0001-01-01T00:00:00``.
- ``resolution``: the 'time between stamps', used in case ``data`` is a one-column array (ignored otherwise).
  Accepted values are the same as for the ``data`` property of [duration](#duration) values.
  The default is ``1 hour``.
  If ``resolution`` is itself an array, then it is either trunk or repeated so as to fit ``data``.
- ``ignore_year``: a JSON boolean to indicate whether or not the time-series should apply to *any* year.
  The default is ``false``, unless ``data`` is a one-column array and ``start`` is not given.
- ``repeat``: a JSON boolean whether or not the time-series should repeat cyclically until the *end* of time.
  The default is ``false``, unless ``data`` is a one-column array and ``start`` is not given.

``index_name`` must be a JSON string. If not specified, a default name 't' will be used.

Examples
~~~~~~~~

Dictionary:

.. code-block:: json

   {
     "type": "time_series",
     "data": {
       "2019-01-01T00:00": 1,
       "2019-01-01T01:30": 5,
       "2019-01-01T02:00": 8
     }
   }

Two-column array:

.. code-block:: json

   {

     "type": "time_series",
     "data": [
       ["2019-01-01T00:00", 1],
       ["2019-01-01T00:30", 2],
       ["2019-01-01T02:00", 8]
     ]
   }

One-column array with implicit (default) indices:

.. code-block:: json

   {
     "type": "time_series",
     "data": [1, 2, 3, 5, 8]
   }

One-column array with explicit (custom) indices:

.. code-block:: json

   {
     "type": "time_series",
     "data": [1, 2, 3, 5, 8],
     "index": {
       "start": "2019-01-01T00:00",
       "resolution": "30 minutes",
       "ignore_year": false,
       "repeat": true
     }
   }

Two-column array with named indices:

.. code-block:: json

   {

     "type": "time_series",
     "data": [
       ["2019-01-01T00:00", 1],
       ["2019-01-01T00:30", 2],
       ["2019-01-01T02:00", 8]
     ],
     "index_name": "Time stamps"
   }

Array
-----

If the ``type`` property is ``array``, then the ``data`` property specifies a one dimensional array.
This is a list of values with zero based indexing.
All values are of the same type which is specified by an optional ``value_type`` property.
If specified, ``value_type`` must be one of the following: ``float``, ``str``, ``duration``, or ``date_time``.
If omitted, ``value_type`` defaults to ``float``

The ``data`` property must be a JSON list. The elements depend on ``value_type``:

- If ``value_type`` is ``float`` then all elements in ``data`` must be JSON numbers.
- If ``value_type`` is ``str`` then all elements in ``data`` must be JSON strings.
- If ``value_type`` is ``duration`` then all elements in ``data`` must be single extensions of time.
- If ``value_type`` is ``date_time`` then all elements in ``data`` must be JSON strings
  in the `ISO8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format.

An array may have an additional property, ``index_name``.
``index_name`` must be a JSON string. If not specified, a default name 'i' will be used.


Examples
~~~~~~~~

An array of numbers:

.. code-block:: json

   {
     "type": "array",
     "data": [2.3, 23.0, 5.0]
   }

An array of durations:

.. code-block:: json

   {
     "type": "array",
     "value_type": "duration",
     "data": ["3 months", "2Y", "4 minutes"]
   }

An array of strings with index name:

.. code-block:: json

   {
     "type": "array",
     "data": ["one", "two"],
     "index_name": "step"
   }


Map
---

If the ``type`` property is ``map``, then the ``data`` property specifies indexed array data.
An additional ``index_type`` specifies the type of the index and must be one of the following:
``float``, ``str``, ``duration``, or ``date_time``.

The ``data`` property can be a JSON mapping with the following properties:

- Every key in the map must be a scalar of the same type as given by ``index_type``:

  * floats are represented by JSON numbers, e.g. ``5.5``
  * strings are represented by JSON strings, e.g. ``"key_1"``
  * durations are represented by duration strings, e.g. ``"1 hour"``.
    Note that *variable* durations are not supported
  * datetimes are represented by ISO8601 time stamps, e.g. ``"2020-01-01T12:00"``

- Every value in the map can be

  * a float, e.g. ``5.5``
  * a duration, e.g. ``{"type": "duration", "data": "3 days"}``
  * a datetime, e.g. ``{"type": "date_time", "data": "2020-01-01T12:00"``}
  * a map, e.g. ``{"type": "map", "index_type": "str", "data":{"a": 2, "b": 3}}``
  * any of the following: time-series, array, time-pattern

Optionally, the ``data`` property can be a two-column JSON array
where the first element is the key and the second the value.

A map may have an additional property, ``index_name``.
``index_name`` must be a JSON string. If not specified, a default name 'x' will be used.

Examples
~~~~~~~~

Dictionary:

.. code-block:: json

   {
     "type": "map",
     "index_type": "date_time",
     "data": {
       "2010-01-01T00:00": {
         "type": "map",
         "index_type": "duration",
         "data": [["1D", -1.0], ["1D", -1.5]]
       },
       "2010-02-01-T00:00": {
         "type": "map",
         "index_type": "duration",
         "data": [["1 month", 2.3], ["2 months", 2.5]]
       }
     }
   }

Two-column array:

.. code-block:: json

   {
     "type": "map",
     "index_type": "str",
     "data": [["cell_1", 1.0], ["cell_2", 2.0], ["cell_3", 3.0]]
   }

Stochastic time series corresponding to the table below:

================ ================ =================== =====
Forecast time    Target time      Stochastic scenario Value
================ ================ =================== =====
2020-04-17T08:00 2020-04-17T08:00 0                   23.0
2020-04-17T08:00 2020-04-17T09:00 0                   24.0
2020-04-17T08:00 2020-04-17T10:00 0                   25.0
2020-04-17T08:00 2020-04-17T08:00 1                   5.5
2020-04-17T08:00 2020-04-17T09:00 1                   6.6
2020-04-17T08:00 2020-04-17T10:00 1                   7.7
================ ================ =================== =====

.. code-block:: json

   {
     "type": "map",
     "index_type": "date_time",
     "index_name": "Forecast time",
     "data": [
       ["2020-04-17T08:00",
        {"type": "map", "index_type": "date_time", "index_name": "Target time", "data": [
          [
            "2020-04-17T08:00", {"type": "map",
                                 "index_type": "float",
                                 "index_name": "Stochastic scenario",
                                 "data": [[0, 23.0], [1, 5.5]]}
          ],
          [
            "2020-04-17T09:00", {"type": "map",
                                 "index_type": "float",
                                 "index_name": "Stochastic scenario",
                                 "data": [[0, 24.0], [1, 6.6]]}
          ],
          [
            "2020-04-17T10:00", {"type": "map",
                                 "index_type": "float",
                                 "index_name": "Stochastic scenario",
                                 "data": [[0, 25.0], [1, 7.7]]}
          ]
        ]}
       ]
     ]
   }
