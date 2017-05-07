import os.path as p
import unittest
import time
import datetime

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance('instance', ['configs/graphite_rollup.xml'])


def setUpModule():
    cluster.up()
    instance.query('CREATE DATABASE test')

def tearDownModule():
    cluster.down()


def compare_with_reference(sql_file, reference_file):
    current_dir = p.dirname(__file__)
    with open(p.join(current_dir, sql_file)) as sql, open(p.join(current_dir, reference_file)) as reference:
        assert instance.query(sql.read()) == reference.read()

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
        compare_with_reference('test3.sql', 'test3.reference')

    def test4(self):
        compare_with_reference('test4.sql', 'test4.reference')
