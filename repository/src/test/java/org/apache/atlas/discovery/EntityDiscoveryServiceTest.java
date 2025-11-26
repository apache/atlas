package org.apache.atlas.discovery;

import junit.framework.TestCase;
import org.apache.atlas.model.discovery.IndexSearchParams;

import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery.CLIENT_ORIGIN_PRODUCT;

/**
 * Unit tests for EntityDiscoveryService.optimizeQueryIfApplicable
 */
public class EntityDiscoveryServiceTest extends TestCase {
    
    /**
     * Test that optimizeQueryIfApplicable does NOT optimize the query 
     * when enableFullRestriction is true.
     * 
     * This test actually invokes the method and verifies that when ABAC full 
     * restriction is enabled, the query remains unchanged (early return behavior).
     */
    public void testOptimizeQueryIfApplicable_WithFullRestrictionEnabled() {
        // Arrange
        IndexSearchParams searchParams = new IndexSearchParams();
        
        // Set up a query DSL that would normally be optimized
        Map<String, Object> dsl = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        query.put("match_all", new HashMap<>());
        dsl.put("query", query);
        
        searchParams.setDsl(dsl);
        searchParams.setEnableFullRestriction(true);
        
        // Store original query for comparison
        String queryBeforeOptimization = searchParams.getQuery();
        assertNotNull("Query should not be null", queryBeforeOptimization);
        
        // Act - invoke the actual method through our testable service
        TestableEntityDiscoveryService service = new TestableEntityDiscoveryService();
        service.optimizeQueryIfApplicable(searchParams, CLIENT_ORIGIN_PRODUCT);
        
        // Assert - query should remain unchanged due to early return
        String queryAfterOptimization = searchParams.getQuery();
        assertEquals("Query MUST NOT be optimized when enableFullRestriction is true", 
                     queryBeforeOptimization, 
                     queryAfterOptimization);
        assertTrue("Query should contain original structure", 
                   queryAfterOptimization.contains("match_all"));
    }
    
    /**
     * Test with a different query structure to ensure the check works 
     * with more complex queries.
     */
    public void testOptimizeQueryIfApplicable_FullRestrictionOverridesClientOrigin() {
        // Arrange
        IndexSearchParams searchParams = new IndexSearchParams();
        
        Map<String, Object> dsl = new HashMap<>();
        Map<String, Object> boolQuery = new HashMap<>();
        boolQuery.put("must", Map.of("term", Map.of("name", "test")));
        dsl.put("query", Map.of("bool", boolQuery));
        
        searchParams.setDsl(dsl);
        searchParams.setEnableFullRestriction(true);
        
        String queryBeforeOptimization = searchParams.getQuery();
        
        // Act - even with PRODUCT origin (which normally triggers optimization),
        // full restriction should prevent it
        TestableEntityDiscoveryService service = new TestableEntityDiscoveryService();
        service.optimizeQueryIfApplicable(searchParams, CLIENT_ORIGIN_PRODUCT);
        
        // Assert
        String queryAfterOptimization = searchParams.getQuery();
        assertEquals("Full restriction should prevent optimization regardless of client origin", 
                     queryBeforeOptimization, 
                     queryAfterOptimization);
    }
    
    /**
     * Testable subclass that uses the special testing constructor.
     * This allows us to test protected methods without full dependency initialization.
     */
    private static class TestableEntityDiscoveryService extends EntityDiscoveryService {
        
        public TestableEntityDiscoveryService() {
            // Use the special testing constructor that skips complex initialization
            super(true);
        }
        
        // Expose the protected method for testing
        @Override
        protected void optimizeQueryIfApplicable(org.apache.atlas.model.discovery.SearchParams searchParams, 
                                                  String clientOrigin) {
            super.optimizeQueryIfApplicable(searchParams, clientOrigin);
        }
    }
}

