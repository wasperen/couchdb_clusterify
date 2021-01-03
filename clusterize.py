import sys
import argparse
import requests

_ENDPOINT = 'http://{node}:5984/_cluster_setup'


def init_session(node, admin, password):
    session = requests.Session()
    r = session.post(f'http://{node}:5984/_session', json=dict(name=admin, password=password))
    print(session.cookies.get_dict())
    return session


def enable_cluster(session, node, admin, password, nr_nodes):
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


def add_node(session, node1, other_node, admin, password, nr_nodes):
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
    response1 = session.post(_ENDPOINT.format(node=node1), json=data)

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
    response2 = session.post(_ENDPOINT.format(node=node1), json=data)
    return response1, response2


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

    args = parser.parse_args()

    other_nodes = [args.node2, args.node3, *args.nodes]
    nr_nodes = 1 + len(other_nodes)

    # init session
    SESSION = init_session(args.node1, args.admin, args.password)

    # add nodes together in cluster
    for node in other_nodes:
        print(f'enable clustering for first node {args.node1}')
        r = enable_cluster(SESSION, args.node1, args.admin, args.password, nr_nodes)
        print(r.json())

        print(f'add node {node} to first node {args.node1}')
        r1, r2 = add_node(SESSION, args.node1, node, args.admin, args.password, nr_nodes)
        print(r1.json())
        print(r2.json())

    # finalize cluster
    print(f'finalizing clustering via first node {args.node1}')
    r = finish_cluster(SESSION, args.node1)
    print(r.json())

    SESSION.close()
