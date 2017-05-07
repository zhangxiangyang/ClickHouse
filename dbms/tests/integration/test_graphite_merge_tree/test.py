import os.path as p
import unittest
import time
import datetime

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance('instance', ['configs/graphite_rollup.xml'])


def setUpModule():
    cluster.up()
    instance.client.query('CREATE DATABASE test')

def tearDownModule():
    cluster.down()


def compare_with_reference(sql_file, reference_file):
    current_dir = p.dirname(__file__)
    with open(p.join(current_dir, sql_file)) as sql, open(p.join(current_dir, reference_file)) as reference:
        assert instance.client.query(sql.read()) == reference.read()

class Test(unittest.TestCase):
    def setUp(self):
        instance.client.query('''
DROP TABLE IF EXISTS test.graphite;
CREATE TABLE test.graphite
    (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
    ENGINE = GraphiteMergeTree(date, (metric, timestamp), 8192, 'graphite_rollup');
''')

    def tearDown(self):
        instance.client.query('DROP TABLE test.graphite')

    def test1(self):
        timestamp = int(time.time())
        timestamp = timestamp - timestamp % 60
        date = datetime.date.today().isoformat()

        q = instance.client.query

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
        compare_with_reference('test2.sql', 'test2.reference')

    def test3(self):
        compare_with_reference('test3.sql', 'test3.reference')

    def test4(self):
        compare_with_reference('test4.sql', 'test4.reference')
