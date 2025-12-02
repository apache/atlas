package org.apache.atlas.idgenerator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Distributed ID Generator that creates unique 7-character alphanumeric sequential IDs across multiple servers.
 * Each server gets a unique prefix to avoid collisions.
 * Assume id = "abcdefg"
 *      a -> Cluster prefix
 *      b -> Server prefix for id generation
 *      cdefg -> actual character supports rollover for next id generation
 */


public class DistributedIdGenerator implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DistributedIdGenerator.class);
    private static final String KEYSPACE = "id_generator";
    private static final String TABLE = "server_ids";
    private static final String SERVER_PREFIX_TABLE = "server_prefix";
    private static final int ID_LENGTH = 7;
    private static final long BATCH_SIZE = 5_000;

    // Server range management
    private static final Lock RANGE_LOCK = new ReentrantLock();
    private static final Map<String, Boolean> ALLOCATED_PREFIXES = new ConcurrentHashMap<>();

    private final String serverName;
    private final CqlSession session;
    private PreparedStatement upsertIDSStmt;
    private PreparedStatement selectIDSStmt;
    private PreparedStatement upsertPrefixStmt;
    private PreparedStatement selectAllPrefixesStmt;

    private final AtomicLong counter = new AtomicLong(0);
    private final AtomicReference<String> currentId;
    private String currentRangePrefix;

    @Deprecated
    public void setCurrentId(String id) {
        this.currentId.set(id);
    }

    // Character set: a-z, A-Z, 0-9
    private static final char[] CHARS = new char[62];
    private static final int BASE = CHARS.length;

    static {
        int index = 0;
        // Add lowercase letters
        for (char c = 'a'; c <= 'z'; c++) {
            CHARS[index++] = c;
        }
        // Add uppercase letters
        for (char c = 'A'; c <= 'Z'; c++) {
            CHARS[index++] = c;
        }
        // Add digits
        for (char c = '0'; c <= '9'; c++) {
            CHARS[index++] = c;
        }
    }

    private static final int MAX_RETRIES = 10;
    private static final long RETRY_DELAY_MS = 2000; // 2 seconds

    public DistributedIdGenerator(String contactPoints, int port, String serverName) {
        this(contactPoints, port, serverName, null, null);
    }

    public DistributedIdGenerator(String contactPoints, int port, String serverName, String username, String password) {
        this.serverName = serverName;
        this.currentId = new AtomicReference<>(); // initialize

        // Initialize Cassandra session with retry logic
        this.session = createSessionWithRetry(contactPoints, port, username, password);

        // Initialize with retry logic
        initializeWithRetry();

        // Initialize the current ID and range
        String initialId = initializeId();

        logger.info("Initialized ID generator for server '{}' with starting ID: {}", serverName, initialId);
    }


    /**
     * Convert a base-62 string to a decimal number
     * @param base62currentId The base-62 string to convert (case-sensitive)
     * @return The decimal value of the base-62 string
     */
    public static long base62ToDecimal(String base62currentId) {
        long result = 0;
        for (int i = 0; i < base62currentId.length(); i++) {
            char c = base62currentId.charAt(i);
            int value = getCharIndex(c);
            result = result * BASE + value;
        }
        return result;
    }

    /**
     * Convert a decimal number to a base-62 string of specified length
     * @param decimal The decimal number to convert
     * @param length The desired length of the output string (will be left-padded with 'a' if needed)
     * @return The base-62 string representation
     */
    public static String decimalToBase62(long decimal, int length) {
        if (decimal < 0) {
            throw new IllegalArgumentException("Decimal must be non-negative");
        }

        char[] result = new char[length];
        Arrays.fill(result, 'a');

        for (int i = length - 1; i >= 0 && decimal > 0; i--) {
            int remainder = (int)(decimal % BASE);
            result[i] = CHARS[remainder];
            decimal = decimal / BASE;
        }

        return new String(result);
    }

    /**
     * Add a decimal number to a base-62 ID and return the result
     * @param base62Id The base-62 ID to add to
     * @param increment The decimal number to add
     * @return The resulting base-62 ID, or null if it would exceed the range
     */
    public static String addDecimalToBase62(String base62Id, long increment) {
        long decimal = base62ToDecimal(base62Id);
        long result = decimal + increment;
        return decimalToBase62(result, base62Id.length());
    }

    /**
     * Add a base-62 number to another base-62 number
     * @param base62Id1 The first base-62 number
     * @param base62Increment The base-62 number to add
     * @return The resulting base-62 number, or null if it would exceed the range
     */
    public static String addBase62(String base62Id1, String base62Increment) {
        long dec1 = base62ToDecimal(base62Id1);
        long dec2 = base62ToDecimal(base62Increment);
        return decimalToBase62(dec1 + dec2, base62Id1.length());
    }

    /**
     * Generate the next ID in the sequence.
     * @return The next unique ID
     */
    public String getCurrentId() {
        return currentId.get();
    }

    public String getCurrentRangePrefix() {
        return currentRangePrefix;
    }

    public synchronized String nextId() {
        String current = currentId.get();

        if (this.counter.get() == 0) {
            counter.incrementAndGet();
            logger.info("Generated next ID: {}", current);
            return current;
        }

        String nextId = incrementId(current);

        // Check if we've exhausted the current range (second character changed)
        if (nextId == null || nextId.charAt(1) != currentRangePrefix.charAt(0)) {
            // Release the current range
            //releaseCurrentRange();

            // Allocate a new range
            nextId = allocateNewPrefix();
            logger.info("Advanced to new range with second character '{}' for server '{}' (ID: {})",
                    currentRangePrefix, serverName, nextId);
        }

        logger.info("Generated next ID: {}", nextId);
        return nextId;
    }

    /**
     * Close the ID generator and release resources.
     * Implements AutoCloseable to allow use in try-with-resources.
     */
    @Override
    public void close() {
        if (session != null) {
            try {
                session.close();
            } catch (Exception e) {
                logger.error("Error closing Cassandra session: {}", e.getMessage(), e);
            }
        }

        logger.info("Closed ID generator for server: {}", serverName);
    }

    private CqlSession createSessionWithRetry(String contactPoints, int port, String username, String password) {
        int attempts = 0;
        long retryDelayMs = 2000; // Start with 2 seconds
        final long maxRetryDelayMs = 30000; // Max 30 seconds between retries

        while (true) {
            try {
                logger.info("Attempting to create Cassandra session (attempt {}/{}) to {}:{}",
                        attempts + 1, MAX_RETRIES, contactPoints, port);

                // Create session builder with common configuration
                CqlSessionBuilder sessionBuilder = CqlSession.builder()
                        .addContactPoint(new InetSocketAddress(contactPoints, port))
                        .withLocalDatacenter("datacenter1")
                        .withConfigLoader(
                                DriverConfigLoader.programmaticBuilder()
                                        .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10))
                                        .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(15))
                                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15))
                                        .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(20))
                                        .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofMillis(500))
                                        .withDuration(DefaultDriverOption.REQUEST_TRACE_ATTEMPTS, Duration.ofSeconds(20))
                                        .build());

                // Add authentication if credentials are provided
                if (username != null && !username.isEmpty() && password != null) {
                    logger.debug("Using authentication with username: {}", username);
                    sessionBuilder = sessionBuilder.withAuthCredentials(username, password);
                } else {
                    logger.debug("No authentication credentials provided, connecting without authentication");
                }

                // Configure the session with additional settings
                return sessionBuilder.build();

            } catch (Exception e) {
                attempts++;
                if (attempts >= MAX_RETRIES) {
                    throw new RuntimeException("Failed to create Cassandra session after " + attempts + " attempts", e);
                }

                // Exponential backoff with jitter
                long delay = Math.min(retryDelayMs, maxRetryDelayMs);
                delay = (long)(delay * (0.9 + 0.2 * Math.random())); // Add jitter

                logger.warn("Failed to create Cassandra session (attempt {}/{}). Retrying in {}ms... Error: {}",
                        attempts, MAX_RETRIES, delay, e.getMessage());

                try {
                    Thread.sleep(delay);
                    retryDelayMs = Math.min(retryDelayMs * 2, maxRetryDelayMs); // Double the delay for next time
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting to retry Cassandra session creation", ie);
                }
            }
        }
    }

    private void initializeWithRetry() {
        int attempts = 0;
        while (attempts < MAX_RETRIES) {
            try {
                // Initialize the schema if it doesn't exist
                initializeSchema();

                // Load existing ranges
                loadAllocatedPrefixes();

                // Prepare statements
                this.upsertIDSStmt = session.prepare(
                        String.format("INSERT INTO %s.%s (server_name, last_id, last_updated) VALUES (?, ?, ?)",
                                KEYSPACE, TABLE));

                this.selectIDSStmt = session.prepare(
                        String.format("SELECT last_id FROM %s.%s WHERE server_name = ?",
                                KEYSPACE, TABLE));

                this.upsertPrefixStmt = session.prepare(
                        String.format("INSERT INTO %s.%s (range_prefix, server_name, last_updated) VALUES (?, ?, ?)",
                                KEYSPACE, SERVER_PREFIX_TABLE));

                this.selectAllPrefixesStmt = session.prepare(
                        String.format("SELECT range_prefix, server_name FROM %s.%s",
                                KEYSPACE, SERVER_PREFIX_TABLE));

                // If we get here, initialization was successful
                return;

            } catch (Exception e) {
                attempts++;
                if (attempts >= MAX_RETRIES) {
                    throw new RuntimeException("Failed to initialize after " + attempts + " attempts", e);
                }
                logger.warn("Initialization failed (attempt {}/{}). Retrying in {}ms...",
                        attempts, MAX_RETRIES, RETRY_DELAY_MS, e);
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during initialization", ie);
                }
            }
        }
    }

    private void initializeSchema() {
        // Create keyspace if it doesn't exist
        session.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s " +
                        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} " +
                        "AND durable_writes = true", KEYSPACE));

        // Create server state table if it doesn't exist
        session.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "    server_name text PRIMARY KEY," +
                        "    last_id text," +
                        "    last_updated timestamp" +
                        ")", KEYSPACE, TABLE));

        // Create server ranges table if it doesn't exist
        session.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "    range_prefix text PRIMARY KEY," +
                        "    server_name text," +
                        "    last_updated timestamp" +
                        ")", KEYSPACE, SERVER_PREFIX_TABLE));
    }

    private void loadAllocatedPrefixes() {
        RANGE_LOCK.lock();
        try {
            if (selectAllPrefixesStmt == null) {
                logger.warn("selectAllPrefixesStmt is null, preparing statement...");
                selectAllPrefixesStmt = session.prepare(
                        String.format("SELECT range_prefix, server_name FROM %s.%s",
                                KEYSPACE, SERVER_PREFIX_TABLE));
            }

            // Clear existing ranges to avoid duplicates
            ALLOCATED_PREFIXES.clear();

            // Load all allocated ranges from the database
            for (Row row : session.execute(selectAllPrefixesStmt.bind())) {
                String rangePrefix = row.getString("range_prefix");
                String ownerServer = row.getString("server_name");

                // Mark the range as allocated
                ALLOCATED_PREFIXES.put(rangePrefix, true);
                logger.info("Loaded range '{}' allocated to server '{}'", rangePrefix, ownerServer);

                // If this is our range, set it as current
                if (serverName.equals(ownerServer)) {
                    logger.info("Found existing range '{}' for this server ('{}')", rangePrefix, serverName);
                    this.currentRangePrefix = rangePrefix;
                }
            }
        } catch (Exception e) {
            logger.error("Error loading allocated ranges: {}", e.getMessage(), e);
            // Re-throw to fail fast if we can't load ranges
            throw new RuntimeException("Failed to load allocated ranges", e);
        } finally {
            RANGE_LOCK.unlock();
        }
    }

    private String allocateNewPrefix() {
        RANGE_LOCK.lock();
        try {
            // Reload ranges to ensure we have the latest state
            loadAllocatedPrefixes();

            // Find the next available second character range
            for (char aChar : CHARS) {
                String rangeKey = String.valueOf(aChar);

                if (ALLOCATED_PREFIXES.containsKey(rangeKey)) {
                    if (aChar == CHARS[CHARS.length-1]) {
                        String message = "All Chars exhausted, can not generate new prefix, please consider increasing length for ID";
                        logger.error(message);
                        throw new RuntimeException(message);
                    }
                    continue;
                }


                // For new ranges, the first character is 'a' and second character is the range key
                String newId = "a" + rangeKey + "a".repeat(ID_LENGTH - 2);

                try {
                    // Use a lightweight transaction to ensure atomic range allocation
                    String query = String.format(
                            "INSERT INTO %s.%s (range_prefix, server_name, last_updated) " +
                                    "VALUES (?, ?, ?) IF NOT EXISTS",
                            KEYSPACE, SERVER_PREFIX_TABLE);

                    PreparedStatement stmt = session.prepare(query);
                    ResultSet result = session.execute(stmt.bind(
                            rangeKey,
                            serverName,
                            Instant.now()
                    ).setConsistencyLevel(ConsistencyLevel.ONE));

                    // Check if the insert was applied (no conflict)
                    if (result.wasApplied()) {
                        ALLOCATED_PREFIXES.put(rangeKey, true);
                        this.currentId.set(newId);
                        this.currentRangePrefix = rangeKey;
                        logger.info("Successfully allocated new range '{}' for server '{}' (ID: {})",
                                rangeKey, serverName, newId);

                        persistCurrentId();

                        return newId;
                    } else {
                        // Another server took this range, mark it as allocated and continue
                        ALLOCATED_PREFIXES.put(rangeKey, true);
                        logger.debug("Range '{}' was just taken by another server, trying next range", rangeKey);
                    }
                } catch (Exception e) {
                    logger.error("Error while trying to allocate range '{}': {}", rangeKey, e.getMessage(), e);
                    // Continue to next range on error
                }
            }

            throw new IllegalStateException("No available ID ranges left. All ranges are allocated.");
        } finally {
            RANGE_LOCK.unlock();
        }
    }

    private String initializeId() {
        // Try to load the last used ID from the database
        Row row = session.execute(selectIDSStmt.bind(serverName)).one();

        if (row != null && !row.isNull("last_id")) {
            // Get the last used ID
            String lastId = row.getString("last_id");

            if (lastId.length() != ID_LENGTH) {
                logger.warn("Invalid ID length in database ({}), expected {}. Allocating new range...",
                        lastId.length(), ID_LENGTH);
                return allocateNewPrefix();
            }

            this.currentRangePrefix = lastId.substring(1, 2); // Get the second character as range prefix

            // Calculate the next ID by incrementing by BATCH_SIZE
            String nextId = incrementRange(lastId);
            if (nextId == null) {
                // If we've exhausted the current range, allocate a new one
                logger.info("Exhausted current range, allocating new range after incrementing by {}", BATCH_SIZE);
                return allocateNewPrefix();
            }

            logger.info("Resuming with ID: {} (incremented by {} from last ID: {}) for server '{}'",
                    nextId, BATCH_SIZE, lastId, serverName);
            return nextId;
        } else {
            // Allocate a new prefix for a fresh start
            String newId = allocateNewPrefix();
            if (newId.length() != ID_LENGTH) {
                throw new IllegalStateException("Allocated ID has invalid length: " + newId);
            }
            logger.info("Allocated new prefix with second character '{}' for server '{}' (ID: {})",
                    currentRangePrefix, serverName, newId);

            return newId;
        }
    }

    /**
     * Persist the current ID to the database.
     */
    public synchronized void persistCurrentId() {
        try {
            session.execute(upsertIDSStmt.bind(
                    serverName,
                    currentId.get(),
                    Instant.now()
            ));
            logger.debug("Persisted ID {} for server {}", currentId.get(), serverName);
        } catch (Exception e) {
            logger.error("Failed to persist current ID: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to persist ID", e);
        }
    }


    /**
     * More efficient Increment an ID by a specified number of positions using base-62 arithmetic
     * NOTE: this is a custom base-62 of {[a-z],[A-Z],[0-9]}
     * @param id The base ID to increment (must be exactly ID_LENGTH characters)
     * @return The new ID after incrementing, or null if the range is exhausted
     */
    private String incrementId(String id) {
        try {
            // Convert the ID to a decimal number, add the increment, and convert back
            String result = addDecimalToBase62(id, 1);

            // Check if we've changed the range prefix (second character)
            if (result.charAt(1) != id.charAt(1)) {
                return null; // Prefix exhausted
            }

            counter.incrementAndGet();
            this.currentId.set(result);

            if (counter.get() % BATCH_SIZE  == 1) {
                persistCurrentId();
            }

            return result;
        } catch (IllegalArgumentException e) {
            // Handle overflow or invalid input
            return null;
        }
    }

    private String incrementRange(String lastId) {
        try {
            // Convert the ID to a decimal number, add the increment, and convert back
            String result = addDecimalToBase62(lastId, BATCH_SIZE);

            // Check if we've changed the range prefix (second character)
            if (result.charAt(1) != lastId.charAt(1)) {
                return null; // Prefix exhausted
            }

            this.currentId.set(result);
            persistCurrentId();

            return result;
        } catch (IllegalArgumentException e) {
            // Handle overflow or invalid input
            return null;
        }
    }

    /**
     * Get the index of a character in our character set.
     */
    private static int getCharIndex(char c) {
        if (c >= 'a' && c <= 'z') {
            return c - 'a';
        } else if (c >= 'A' && c <= 'Z') {
            return 26 + (c - 'A');
        } else if (c >= '0' && c <= '9') {
            return 52 + (c - '0');
        }
        throw new IllegalArgumentException("Invalid character in ID: " + c);
    }
}