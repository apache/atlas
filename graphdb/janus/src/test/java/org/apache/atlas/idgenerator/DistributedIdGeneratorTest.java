package org.apache.atlas.idgenerator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.atlas.exception.AtlasBaseException;
import org.springframework.core.annotation.Order;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DistributedIdGeneratorTest {

    private String serverName = "test-server-0";

    private static CqlSession session;
    private static final String KEY = "id_generator";
    private static final String TAB_IDS = "server_ids";
    private static final String TAB_PREFIX = "server_prefix";

    static {
        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("localhost", 9042))
                .withLocalDatacenter("datacenter1")
                .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                        .withString(DefaultDriverOption.REQUEST_TIMEOUT, "30 seconds")
                        .build())
                .build();

        session.execute("drop KEYSPACE if exists %s".formatted(KEY));

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Order(1)
    void validateCloseAtBatchSize() {

        DistributedIdGenerator idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        String prefix = idGenerator.getCurrentRangePrefix();

        assertEquals("a%saaaaa".formatted(prefix), idGenerator.nextId()); // 1
        assertEquals("a%saaaab".formatted(prefix), idGenerator.nextId()); // 2
        assertEquals("a%saaaac".formatted(prefix), idGenerator.nextId()); // 3
        assertEquals("a%saaaad".formatted(prefix), idGenerator.nextId()); // 4

        int rowCount = validatePrefixTable(prefix, serverName);
        assertEquals(1, rowCount);

        rowCount = validateIDsTable(prefix, serverName, "a%saaaaa");
        assertEquals(1, rowCount);

        assertEquals("a%saaaae".formatted(prefix), idGenerator.nextId());  // 5 -> Exhaust batchSize

        rowCount = validatePrefixTable(prefix, serverName);
        assertEquals(1, rowCount);

        rowCount = validateIDsTable(prefix, serverName, "a%saaaaa");
        assertEquals(1, rowCount);

        // close to simulate Atlas restart event
        idGenerator.close();

        // Exhaust batch
        idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        assertEquals("a%saaaaf".formatted(prefix), idGenerator.nextId()); // 6

        validateIDsTable(prefix, serverName, "a%saaaaf");
    }

    @Test
    @Order(2)
    void validateCloseBeforeBatchSize() {
        String serverName = "test-server-2";

        DistributedIdGenerator idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        String prefix = idGenerator.getCurrentRangePrefix();

        assertEquals("a%saaaaa".formatted(prefix), idGenerator.nextId()); // 1
        assertEquals("a%saaaab".formatted(prefix), idGenerator.nextId()); // 2

        int rowCount = validatePrefixTable(prefix, serverName);
        assertEquals(1, rowCount);

        rowCount = validateIDsTable(prefix, serverName, "a%saaaaa");
        assertEquals(1, rowCount);

        // Close to simulate Atlas restart event
        // This should lead to wastage on 3 IDs
        idGenerator.close();


        rowCount = validatePrefixTable(prefix, serverName);
        assertEquals(1, rowCount);

        rowCount = validateIDsTable(prefix, serverName, "a%saaaaa");
        assertEquals(1, rowCount);


        // Simulate starting server
        idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        assertEquals("a%saaaaf".formatted(prefix), idGenerator.nextId()); //

        rowCount = validatePrefixTable(prefix, serverName);
        assertEquals(1, rowCount);

        validateIDsTable(prefix, serverName, "a%saaaaf");
    }

    @Test
    @Order(3)
    void validateCloseAfterBatchSize() throws AtlasBaseException, IOException, InterruptedException {
        String serverName = "test-server-3";

        DistributedIdGenerator idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        String prefix = idGenerator.getCurrentRangePrefix();

        assertEquals("a%saaaaa".formatted(prefix), idGenerator.nextId()); // 1
        assertEquals("a%saaaab".formatted(prefix), idGenerator.nextId()); // 2
        assertEquals("a%saaaac".formatted(prefix), idGenerator.nextId()); // 3
        assertEquals("a%saaaad".formatted(prefix), idGenerator.nextId()); // 4
        assertEquals("a%saaaae".formatted(prefix), idGenerator.nextId()); // 5 -> Exhaust batchSize
        assertEquals("a%saaaaf".formatted(prefix), idGenerator.nextId()); // 6
        assertEquals("a%saaaag".formatted(prefix), idGenerator.nextId()); // 7

        int rowCount = validatePrefixTable(prefix, serverName);
        assertEquals(1, rowCount);

        rowCount = validateIDsTable(prefix, serverName, "a%saaaaf");
        assertEquals(1, rowCount);

        // Close to simulate Atlas restart event
        idGenerator.close();

        // New object to simulate Restart Atlas
        idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        assertEquals("a%saaaak".formatted(prefix), idGenerator.nextId()); // 6

        validateIDsTable(prefix, serverName, "a%saaaak");

        assertEquals("a%saaaal".formatted(prefix), idGenerator.nextId()); // 7
    }

    @Test
    @Order(4)
    void validateCloseMultiple() throws AtlasBaseException, IOException, InterruptedException {
        String serverName = "test-server-4";

        DistributedIdGenerator idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        String prefix = idGenerator.getCurrentRangePrefix();

        assertEquals("a%saaaaa".formatted(prefix), idGenerator.nextId()); // 1
        assertEquals("a%saaaab".formatted(prefix), idGenerator.nextId()); // 2

        // Close to simulate Atlas restart event
        idGenerator.close();

        // New object to simulate Restart Atlas
        idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        assertEquals("a%saaaaf".formatted(prefix), idGenerator.nextId()); // 1
        assertEquals("a%saaaag".formatted(prefix), idGenerator.nextId()); // 2
        assertEquals("a%saaaah".formatted(prefix), idGenerator.nextId()); // 3

        // Close to simulate Atlas restart event
        idGenerator.close();

        // New object to simulate Restart Atlas
        idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);


        assertEquals("a%saaaak".formatted(prefix), idGenerator.nextId()); // 1
        assertEquals("a%saaaal".formatted(prefix), idGenerator.nextId()); // 2

        // Close to simulate Atlas restart event
        idGenerator.close();

        // New object to simulate Restart Atlas
        idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        assertEquals("a%saaaap".formatted(prefix), idGenerator.nextId()); // 1
        assertEquals("a%saaaaq".formatted(prefix), idGenerator.nextId()); // 2
    }

    @Test
    @Order(5)
    void validateSequentialOverflows() throws AtlasBaseException, IOException, InterruptedException {
        String serverName = "test-server-5";

        DistributedIdGenerator idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        String prefix = idGenerator.getCurrentRangePrefix();

        String id = "";

        for (long i = 0; i < 25 ; i++) {
            id = idGenerator.nextId();
        }
        assertEquals("a%saaaay".formatted(prefix), id);
        validateIDsTable(prefix, serverName, "a%saaaau");

        assertEquals("a%saaaaz".formatted(prefix), idGenerator.nextId());
        assertEquals("a%saaaaA".formatted(prefix), idGenerator.nextId());
        validateIDsTable(prefix, serverName, "a%saaaaz");

        assertEquals("a%saaaaB".formatted(prefix), idGenerator.nextId());


        for (long i = 0; i < 24 ; i++) {
            id = idGenerator.nextId();
        }
        assertEquals("a%saaaaZ".formatted(prefix), id);
        validateIDsTable(prefix, serverName, "a%saaaaY");

        assertEquals("a%saaaa0".formatted(prefix), idGenerator.nextId());
        assertEquals("a%saaaa1".formatted(prefix), idGenerator.nextId());
        assertEquals("a%saaaa2".formatted(prefix), idGenerator.nextId());
        assertEquals("a%saaaa3".formatted(prefix), idGenerator.nextId());
        assertEquals("a%saaaa4".formatted(prefix), idGenerator.nextId());
        assertEquals("a%saaaa5".formatted(prefix), idGenerator.nextId());
        assertEquals("a%saaaa6".formatted(prefix), idGenerator.nextId());
        assertEquals("a%saaaa7".formatted(prefix), idGenerator.nextId());
        assertEquals("a%saaaa8".formatted(prefix), idGenerator.nextId());
        assertEquals("a%saaaa9".formatted(prefix), idGenerator.nextId());
        validateIDsTable(prefix, serverName, "a%saaaa8");


        assertEquals("a%saaaba".formatted(prefix), idGenerator.nextId()); // Rollover

        idGenerator.close();
        idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        validateIDsTable(prefix, serverName, "a%saaabd");
        assertEquals("a%saaabd".formatted(prefix), idGenerator.nextId()); // Rollover
        assertEquals("a%saaabe".formatted(prefix), idGenerator.nextId()); // Rollover

        idGenerator.close();
    }

    @Test
    @Order(6)
    void validateRollovers() throws AtlasBaseException, IOException, InterruptedException {
        String serverName = "test-server-6";

        DistributedIdGenerator idGenerator = new DistributedIdGenerator("localhost", 9042, serverName);

        String prefix = idGenerator.getCurrentRangePrefix();

        String id = "";
        long index = 0;

        for (long i = 0; i < 63 ; i++) {
            id = idGenerator.nextId();
        }
        assertEquals("a%saaaba".formatted(prefix), id);
        validateIDsTable(prefix, serverName, "a%saaaa8");

        for (long i = 0; i < 62 ; i++) {
            id = idGenerator.nextId();
        }
        assertEquals("a%saaaca".formatted(prefix), id);


        for (long i = 0; i < 3720 ; i++) {
            id = idGenerator.nextId();
        }
        assertEquals("a%saabaa".formatted(prefix), id);

        for (long i = 0; i < Math.pow(62, 2) ; i++) {
            id = idGenerator.nextId();
        }
        assertEquals("a%saacaa".formatted(prefix), id);

        // Rollover prefix
        idGenerator.setCurrentId("a%s99999".formatted(prefix));
        id = idGenerator.nextId();
        prefix = idGenerator.getCurrentRangePrefix();

        assertEquals("a%saaaaa".formatted(prefix), id);

        // Rollover prefix with multiple servers
        String server0 = "test-server-" + System.currentTimeMillis();
        String server1 = "test-server-" + System.currentTimeMillis();
        new DistributedIdGenerator("localhost", 9042, server0); // prefix: c
        new DistributedIdGenerator("localhost", 9042, server1); // prefix: d
        validatePrefixTable("h", server0);
        validatePrefixTable("i", server1);

        idGenerator.setCurrentId("a%s99999".formatted(prefix));
        id = idGenerator.nextId();
        prefix = idGenerator.getCurrentRangePrefix();

        assertEquals("j", prefix);
        assertEquals("a%saaaaa".formatted(prefix), id);

        // One more Rollover to test-server-6
        idGenerator.setCurrentId("a%s99999".formatted(prefix));
        id = idGenerator.nextId();
        prefix = idGenerator.getCurrentRangePrefix();

        assertEquals("k", prefix);
        assertEquals("a%saaaaa".formatted(prefix), id);

        // Exhaust all prefixes for test-server-6
        for (int i = 11; i <= 61; i++) {
            idGenerator.setCurrentId("a%s99999".formatted(prefix));
            id = idGenerator.nextId();
            prefix = idGenerator.getCurrentRangePrefix();
        }

        assertEquals("9", idGenerator.getCurrentRangePrefix());
        assertEquals("a%saaaaa".formatted(prefix), id);

        try {
            // This should fail with all prefixes exhausted
            idGenerator.setCurrentId("a%s99999".formatted(prefix));
            id = idGenerator.nextId();
        } catch (RuntimeException re) {
            assertTrue(re.getMessage().contains("All Chars exhausted"));
        }

        idGenerator.close();
    }

    private int validateIDsTable(String prefix, String serverName, String id) {
        int rowCount = 0;
        for (Row row : session.execute("select * from %s.%s where server_name = '%s'".formatted(KEY, TAB_IDS, serverName))) {
            String server_name = row.getString("server_name");
            String last_id = row.getString("last_id");

            assertEquals(serverName, server_name);
            assertEquals(last_id, id.formatted(prefix));
            rowCount++;
        }

        return rowCount;
    }

    private int validatePrefixTable(String prefix, String serverName) {
        int rowCount = 0;
        for (Row row : session.execute("select * from %s.%s where range_prefix = '%s'".formatted(KEY, TAB_PREFIX, prefix))) {
            String server_name = row.getString("server_name");

            assertEquals(serverName, server_name);
            rowCount++;
        }

        return rowCount;
    }
}