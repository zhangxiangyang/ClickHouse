import unittest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

instance1 = cluster.add_instance('instance1', [], zookeeper_required=True)
instance2 = cluster.add_instance('instance2', [], zookeeper_required=True)

def setUpModule():
    cluster.up()

def tearDownModule():
    cluster.down()

class Test(unittest.TestCase):
    def test(self):
        pass
