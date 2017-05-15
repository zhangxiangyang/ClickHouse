import os.path as p
import unittest
import time
import datetime
import difflib

from helpers.cluster import ClickHouseCluster
from helpers.client import TSV

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance('instance', ['configs/graphite_rollup.xml'])


def setUpModule():
    cluster.up()
    instance.query('CREATE DATABASE test')

def tearDownModule():
    cluster.down()


class Test(unittest.TestCase):
    def setUp(self):
        instance.query('''
DROP TABLE IF EXISTS test.graphite;
CREATE TABLE test.graphite
    (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
    ENGINE = GraphiteMergeTree(date, (metric, timestamp), 8192, 'graphite_rollup');
''')


    def tearDown(self):
        instance.query('DROP TABLE test.graphite')


    def test1(self):
        timestamp = int(time.time())
        timestamp = timestamp - timestamp % 60
        date = datetime.date.today().isoformat()

        q = instance.query

        q('''
INSERT INTO test.graphite (metric, value, timestamp, date, updated) VALUES ('one_min.x1', 100, {timestamp}, '{date}', 1);
INSERT INTO test.graphite (metric, value, timestamp, date, updated) VALUES ('one_min.x1', 200, {timestamp}, '{date}', 2);
'''.format(timestamp=timestamp, date=date))

        expected1 = '''\
one_min.x1	100	{timestamp}	{date}	1
one_min.x1	200	{timestamp}	{date}	2
'''.format(timestamp=timestamp, date=date)

        self.assertEqual(q('SELECT * FROM test.graphite ORDER BY updated'), expected1)

        q('OPTIMIZE TABLE test.graphite')

        expected2 = '''\
one_min.x1	200	{timestamp}	{date}	2
'''.format(timestamp=timestamp, date=date)

        self.assertEqual(q('SELECT * FROM test.graphite'), expected2)


    def test2(self):
        q = instance.query

        result1 = q('''
SELECT avg(v), max(upd)
FROM (SELECT timestamp,
            argMax(value, (updated, number)) AS v,
            max(updated) AS upd
      FROM (SELECT 'one_min.x5' AS metric,
                   toFloat64(number) AS value,
                   toUInt32(1111111111 + intDiv(number, 3)) AS timestamp,
                   toDate('2017-02-02') AS date,
                   toUInt32(intDiv(number, 2)) AS updated,
                   number
            FROM system.numbers LIMIT 1000000)
      WHERE intDiv(timestamp, 600) * 600 = 1111444200
      GROUP BY timestamp)
''')
        expected1 = '''\
999634.9918367347	499999
'''
        self.assertEqual(result1, expected1)

        result2 = q('''
INSERT INTO test.graphite
    SELECT 'one_min.x' AS metric,
           toFloat64(number) AS value,
           toUInt32(1111111111 + intDiv(number, 3)) AS timestamp,
           toDate('2017-02-02') AS date, toUInt32(intDiv(number, 2)) AS updated
    FROM (SELECT * FROM system.numbers LIMIT 1000000)
    WHERE intDiv(timestamp, 600) * 600 = 1111444200;

OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;

SELECT * FROM test.graphite;
''')
        expected2 = '''\
one_min.x	999634.9918367347	1111444200	2017-02-02	499999
'''
        self.assertEqual(result2, expected2)


    def test3(self):
        result = instance.query('''
INSERT INTO test.graphite
    SELECT 'one_min.x' AS metric,
           toFloat64(number) AS value,
           toUInt32(1111111111 - intDiv(number, 3)) AS timestamp,
           toDate('2017-02-02') AS date,
           toUInt32(100 - number) AS updated
    FROM (SELECT * FROM system.numbers LIMIT 50);

OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;

SELECT * FROM test.graphite;
''')

        expected = '''\
one_min.x	24	1111110600	2017-02-02	100
'''

        self.assertEqual(result, expected)


    def test4(self):
        result = instance.query('''
INSERT INTO test.graphite
    SELECT 'one_min.x' AS metric,
           toFloat64(number) AS value,
           toUInt32(1111111111 + intDiv(number, 3) * 600) AS timestamp,
           toDate('2017-02-02') AS date, toUInt32(100 - number) AS updated
    FROM (SELECT * FROM system.numbers LIMIT 50);

OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;

SELECT * FROM test.graphite;


INSERT INTO test.graphite
    SELECT 'one_min.y' AS metric,
           toFloat64(number) AS value,
           toUInt32(1111111111 + number * 600) AS timestamp,
           toDate('2017-02-02') AS date,
           toUInt32(100 - number) AS updated
    FROM (SELECT * FROM system.numbers LIMIT 50);

OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;

SELECT * FROM test.graphite;
''')

        with open(p.join(p.dirname(__file__), 'test4.reference')) as reference:
            assert TSV(result) == TSV(reference)
