from ..cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__) \
    .add_instance('instance', ['configs/graphite_rollup.xml'])

def setup_module():
    print "Setting up cluster..."
    cluster.up()

def teardown_module():
    print "Shutting down cluster..."
    cluster.down()

def test():
    pass
