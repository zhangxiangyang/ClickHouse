import unittest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__) \
    .add_instance('instance1', [], zookeeper_required=True) \
    .add_instance('instance2', [], zookeeper_required=True) \

def setUpModule():
    print "Setting up cluster..."
    cluster.up()

def tearDownModule():
    print "Shutting down cluster..."
    cluster.down()

class Test(unittest.TestCase):
    def test(self):
        pass
