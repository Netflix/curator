package org.I0Itec.zkclient;

import org.I0Itec.zkclient.testutil.ZkPathUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class AbstractConnectionTest {

    private final IZkConnection _connection;

    public AbstractConnectionTest(IZkConnection connection) {
        _connection = connection;
    }

    @Test
    public void testGetChildren_OnEmptyFileSystem() throws KeeperException, InterruptedException {
        InMemoryConnection connection = new InMemoryConnection();
        List<String> children = connection.getChildren("/", false);
        assertEquals(0, children.size());
    }

    @Test
    @Ignore("I don't understand this test -JZ")
    public void testSequentials() throws KeeperException, InterruptedException {
        String sequentialPath = _connection.create("/a", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
        int firstSequential = Integer.parseInt(sequentialPath.substring(2));
        assertEquals("/a" + ZkPathUtil.leadingZeros(firstSequential++, 10), sequentialPath);
        assertEquals("/a" + ZkPathUtil.leadingZeros(firstSequential++, 10), _connection.create("/a", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL));
        assertEquals("/a" + ZkPathUtil.leadingZeros(firstSequential++, 10), _connection.create("/a", new byte[0], CreateMode.PERSISTENT_SEQUENTIAL));
        assertEquals("/b" + ZkPathUtil.leadingZeros(firstSequential++, 10), _connection.create("/b", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL));
        assertEquals("/b" + ZkPathUtil.leadingZeros(firstSequential++, 10), _connection.create("/b", new byte[0], CreateMode.PERSISTENT_SEQUENTIAL));
        assertEquals("/a" + ZkPathUtil.leadingZeros(firstSequential++, 10), _connection.create("/a", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL));
    }

}
