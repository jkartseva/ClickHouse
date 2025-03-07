import logging
import random
import string

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

NUM_WORKERS = 2


def gen_insert_values(size):
    return ",".join(
        f"({i},'{''.join(random.choices(string.ascii_lowercase, k=5))}')"
        for i in range(size)
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node1",
        main_configs=["configs/storage_conf.xml"],
        with_minio=True,
        env_variables={"ENDPOINT_SUBPATH": "node1"},
        stay_alive=True,
    )
    cluster.add_instance(
        "node2",
        main_configs=["configs/storage_conf.xml"],
        with_minio=True,
        env_variables={"ENDPOINT_SUBPATH": "node2", "ROTATED_REPLICA_ENDPOINT_SUBPATH": "node1"},
        stay_alive=True,
        instance_env_variables=True,
    )

    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test():
    node1 = cluster.instances["node1"]
    def create_insert(node, table_name, insert_values):
        node.query(
            """
            CREATE TABLE {} (
                id Int64,
                data String
            ) ENGINE=MergeTree()
            ORDER BY id
            SETTINGS storage_policy='s3_plain_rewritable'
            """.format(table_name)
        )
        node.query("INSERT INTO {} VALUES {}".format(table_name, insert_values))

    create_insert(node1, 'test1', gen_insert_values(1000))

    assert int(node1.query("SELECT count(*) FROM test1")) == 1000

    uuid1 = node1.query("SELECT uuid FROM system.tables WHERE table='test1'").strip()
    logging.info(f"UUID {uuid1}")

    create_insert(node1, 'test2', gen_insert_values(1000))
    assert int(node1.query("SELECT count(*) FROM test2")) == 1000

    uuid2 = node1.query("SELECT uuid FROM system.tables WHERE table='test2'").strip()
    logging.info(f"UUID {uuid2}")

    node1.query("DETACH TABLE test1")
    node1.query("DETACH TABLE test2")
    node1.stop()

    node2 = cluster.instances["node2"]
    for (name, uuid) in [('test1', uuid1), ('test2', uuid2)]:
        node2.query(
        f'''ATTACH TABLE test_rotated_{name} UUID '{uuid}' (id Int64, data String)
        ENGINE=MergeTree()
        ORDER BY id
        SETTINGS disk=disk(
            name=my_disk,
            type=object_storage,
            object_storage_type=s3,
            metadata_type=plain_rewritable,
            endpoint='http://minio1:9001/root/data/',
            endpoint_subpath='node1',
            access_key_id='minio',
            secret_access_key='minio123')
        ''')

    assert int(node2.query("SELECT count(*) FROM test_rotated_test1")) == 1000
    assert int(node2.query("SELECT count(*) FROM test_rotated_test2")) == 1000

