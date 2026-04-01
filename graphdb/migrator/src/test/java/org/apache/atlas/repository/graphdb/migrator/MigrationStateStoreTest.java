package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class MigrationStateStoreTest {

    @Mock private CqlSession mockSession;
    @Mock private PreparedStatement mockInsertStmt;
    @Mock private PreparedStatement mockSelectCompletedStmt;
    @Mock private PreparedStatement mockSelectPhaseStmt;
    @Mock private BoundStatement mockBoundStmt;
    @Mock private ResultSet mockResultSet;
    @Mock private ResultSet mockEmptyResultSet;

    private MigrationStateStore stateStore;
    private AutoCloseable mocks;

    @BeforeMethod
    public void setUp() {
        mocks = MockitoAnnotations.openMocks(this);

        when(mockSession.execute(anyString())).thenReturn(mockResultSet);
        // Return different PreparedStatements for each prepare call (in order)
        when(mockSession.prepare(anyString()))
            .thenReturn(mockInsertStmt)
            .thenReturn(mockSelectCompletedStmt)
            .thenReturn(mockSelectPhaseStmt);
        when(mockInsertStmt.bind(any())).thenReturn(mockBoundStmt);
        when(mockSelectCompletedStmt.bind(any())).thenReturn(mockBoundStmt);
        when(mockSelectPhaseStmt.bind(any())).thenReturn(mockBoundStmt);
        when(mockSession.execute(any(BoundStatement.class))).thenReturn(mockResultSet);

        stateStore = new MigrationStateStore(mockSession, "test_ks");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    public void testInitCreatesTableAndPrepares() {
        stateStore.init();

        // Verify table creation
        verify(mockSession).execute(contains("CREATE TABLE IF NOT EXISTS test_ks.migration_state"));

        // Verify 3 prepare calls (insert, selectCompleted, selectPhase)
        verify(mockSession, times(3)).prepare(anyString());
    }

    @Test
    public void testMarkRangeStarted() {
        stateStore.init();
        stateStore.markRangeStarted("scan", -100L, 100L);

        verify(mockSession, atLeastOnce()).execute(any(BoundStatement.class));
    }

    @Test
    public void testMarkRangeCompleted() {
        stateStore.init();
        stateStore.markRangeCompleted("scan", -100L, 100L, 500, 200);

        verify(mockSession, atLeastOnce()).execute(any(BoundStatement.class));
    }

    @Test
    public void testMarkRangeFailed() {
        stateStore.init();
        stateStore.markRangeFailed("scan", -100L, 100L);

        verify(mockSession, atLeastOnce()).execute(any(BoundStatement.class));
    }

    @Test
    public void testGetCompletedRangesEmpty() {
        when(mockResultSet.iterator()).thenReturn(Collections.emptyIterator());
        when(mockSession.execute(any(BoundStatement.class))).thenReturn(mockResultSet);

        stateStore.init();
        Set<Long> completed = stateStore.getCompletedRanges("scan");
        assertTrue(completed.isEmpty());
    }

    @Test
    public void testGetCompletedRangesWithResults() {
        Row mockRow1 = mock(Row.class);
        Row mockRow2 = mock(Row.class);
        when(mockRow1.getLong("token_range_start")).thenReturn(-100L);
        when(mockRow2.getLong("token_range_start")).thenReturn(100L);

        ResultSet completedRs = mock(ResultSet.class);
        when(completedRs.iterator()).thenReturn(Arrays.asList(mockRow1, mockRow2).iterator());

        // The 2nd execute(BoundStatement) call should return our completedRs
        when(mockSession.execute(any(BoundStatement.class))).thenReturn(completedRs);

        stateStore.init();
        Set<Long> completed = stateStore.getCompletedRanges("scan");
        assertEquals(completed.size(), 2);
        assertTrue(completed.contains(-100L));
        assertTrue(completed.contains(100L));
    }

    @Test
    public void testGetPhaseSummaryEmpty() {
        when(mockResultSet.iterator()).thenReturn(Collections.emptyIterator());
        when(mockSession.execute(any(BoundStatement.class))).thenReturn(mockResultSet);

        stateStore.init();
        long[] summary = stateStore.getPhaseSummary("scan");
        assertEquals(summary[0], 0); // completedRanges
        assertEquals(summary[1], 0); // totalVertices
        assertEquals(summary[2], 0); // totalEdges
    }

    @Test
    public void testGetPhaseSummaryWithCompletedRanges() {
        Row row1 = mock(Row.class);
        when(row1.getString("status")).thenReturn("COMPLETED");
        when(row1.getLong("vertices_processed")).thenReturn(500L);
        when(row1.getLong("edges_processed")).thenReturn(200L);

        Row row2 = mock(Row.class);
        when(row2.getString("status")).thenReturn("COMPLETED");
        when(row2.getLong("vertices_processed")).thenReturn(300L);
        when(row2.getLong("edges_processed")).thenReturn(100L);

        Row row3 = mock(Row.class);
        when(row3.getString("status")).thenReturn("FAILED");
        when(row3.getLong("vertices_processed")).thenReturn(0L);
        when(row3.getLong("edges_processed")).thenReturn(0L);

        ResultSet phaseRs = mock(ResultSet.class);
        when(phaseRs.iterator()).thenReturn(Arrays.asList(row1, row2, row3).iterator());
        when(mockSession.execute(any(BoundStatement.class))).thenReturn(phaseRs);

        stateStore.init();
        long[] summary = stateStore.getPhaseSummary("scan");
        assertEquals(summary[0], 2);   // 2 completed ranges
        assertEquals(summary[1], 800); // 500 + 300 vertices
        assertEquals(summary[2], 300); // 200 + 100 edges
    }

    @Test
    public void testClearState() {
        stateStore.init();
        stateStore.clearState("scan");

        verify(mockSession).execute(contains("DELETE FROM test_ks.migration_state WHERE phase = 'scan'"));
    }
}
