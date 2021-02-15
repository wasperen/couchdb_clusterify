import sys
import argparse
import requests

_ENDPOINT = 'http://{node}:5984/_cluster_setup'


def init_session(node, admin, password):
    session = requests.Session()
    r = session.post(f'http://{node}:5984/_session', json=dict(name=admin, password=password))
    print(session.cookies.get_dict())
    return session


def local_enable_cluster(session, node, admin, password, nr_nodes):
    # {
    #   "action":"enable_cluster",
    #   "username":"admin",
    #   "password":"admin",
    #   "bind_address":"0.0.0.0",
    #   "port":5984,
    #   "node_count":3,
    #   "singlenode":false
    # }
    # { 'action': 'enable_cluster',
    #   'username': 'admin',
    #   'password': 'admin',
    #   'bind_address': '0.0.0.0',
    #   'port': 5984,
    #   'node_count': 3,
    #   'singlenode': False
    # }
    # {'error': 'bad_request', 'reason': 'Cluster is already enabled'}

    data = dict(
        action='enable_cluster',
        username=admin,
        password=password,
        bind_address='0.0.0.0',
        port=5984,
        node_count=nr_nodes,
        singlenode=False
    )
    print(data)
    return session.post(_ENDPOINT.format(node=node), json=data)


def remote_enable_cluster(session, node1, other_node, admin, password, nr_nodes):
    # {
    #   "action":"enable_cluster",
    #   "username":"admin",
    #   "password":"admin",
    #   "bind_address":"0.0.0.0",
    #   "port":5984,
    #   "node_count":3,
    #   "remote_node":"box02.couch",
    #   "remote_current_user":"admin",
    #   "remote_current_password":"admin"
    # }
    # {
    #   'action': 'enable_cluster',
    #   'username': 'admin',
    #   'password': 'admin',
    #   'bind_address': '0.0.0.0',
    #   'port': 5984,
    #   'node_count': 3,
    #   'remote_node': 'box02.couch',
    #   'remote_current_user': 'admin',
    #   'remote_current_password': 'admin'
    # }
    # {'ok': True}
    data = dict(
        action='enable_cluster',
        username=admin,
        password=password,
        bind_address='0.0.0.0',
        port=5984,
        node_count=nr_nodes,
        remote_node=other_node,
        remote_current_user=admin,
        remote_current_password=password
    )
    print(data)
    return session.post(_ENDPOINT.format(node=node1), json=data)


def add_node(session, node1, other_node, admin, password):
    # {
    #   "action":"add_node",
    #   "username":"admin",
    #   "password":"admin",
    #   "host":"box02.couch",
    #   "port":5984,
    #   "singlenode":false
    # }
    # {
    #   'action': 'add_node',
    #   'username': 'admin',
    #   'password': 'admin',
    #   'host': 'box02.couch',
    #   'port': 5984,
    #   'singlenode': False
    # }
    # {'ok': True}
    data = dict(
        action="add_node",
        username=admin,
        password=password,
        host=other_node,
        port=5984,
        singlenode=False
    )
    print(data)
    return session.post(_ENDPOINT.format(node=node1), json=data)


def finish_cluster(session, node):
    # {"action":"finish_cluster"}
    # {'action': 'finish_cluster'}
    # {'error': 'setup_error', 'reason': 'Cluster setup unable to sync admin passwords'}
    data = dict(
        action='finish_cluster'
    )
    print(data)
    return session.post(_ENDPOINT.format(node=node), json=data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Utility to initialize a CouchDB cluster")
    parser.add_argument('node1', metavar='NODE_NAME1', type=str, help='host name of first cluster node')
    parser.add_argument('node2', metavar='NODE_NAME2', type=str, help='host name of second cluster node')
    parser.add_argument('node3', metavar='NODE_NAME3', type=str, help='host name of third cluster node')
    parser.add_argument('nodes', metavar='NODE_NAME', type=str, nargs='*', help='host names of additional nodes')
    parser.add_argument('-q', '--shards', type=int, required=True, help="number of shards")
    parser.add_argument('-n', '--replicas', type=int, required=True, help="number of replicas of each shard")
    parser.add_argument('--admin', type=str, required=False, default="admin", help="name of admin user")
    parser.add_argument('--password', type=str, required=False, default="admin", help="password of admin user")

    ARGS = parser.parse_args()

    OTHER_NODES = [ARGS.node2, ARGS.node3, *ARGS.nodes]
    NR_NODES = 1 + len(OTHER_NODES)

    # Work around https://github.com/apache/couchdb/issues/2858
    requests.get("http://" + ARGS.node1 + ":5984/")

    # add nodes together in cluster
    for NODE in OTHER_NODES:
        print(f'locally enable clustering for first node {ARGS.node1}')
        SESSION = init_session(ARGS.node1, ARGS.admin, ARGS.password)
        r = local_enable_cluster(SESSION, ARGS.node1, ARGS.admin, ARGS.password, NR_NODES)
        print(r.json())
        SESSION.close()

        print(f'remotely enable clustering for remote node {NODE}')
        SESSION = init_session(ARGS.node1, ARGS.admin, ARGS.password)
        r = remote_enable_cluster(SESSION, ARGS.node1, NODE, ARGS.admin, ARGS.password, NR_NODES)
        print(r.json())

        print(f'add node {NODE} to cluster')
        r = add_node(SESSION, ARGS.node1, NODE, ARGS.admin, ARGS.password)
        print(r.json())
        SESSION.close()

    # finalize cluster
    print(f'finalizing clustering via first node {ARGS.node1}')
    SESSION = init_session(ARGS.node1, ARGS.admin, ARGS.password)
    r = finish_cluster(SESSION, ARGS.node1)
    print(r.json())
    SESSION.close()
