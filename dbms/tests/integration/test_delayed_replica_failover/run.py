from ..helpers import ClickHouseCluster

cluster = ClickHouseCluster(__file__) \
    .add_instance('instance1', [], zookeeper_required=True) \
    .add_instance('instance2', [], zookeeper_required=True) \

def setup_module():
    print "Setting up cluster..."
    cluster.up()

def teardown_module():
    print "Shutting down cluster..."
    cluster.down()

def test():
    pass
