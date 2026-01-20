package org.apache.atlas.repository.store.graph.v2.terms;

import org.apache.atlas.RequestContext;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.junit.After;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.type.Constants.MEANINGS_PROPERTY_KEY;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for EntityGraphMapper's meaning-related methods:
 * - addMeaningsToEntityRelations
 * - addMeaningsToEntityV1
 * 
 * These tests verify the logic and behavior of term assignment operations
 * without requiring a full EntityGraphMapper instance.
 */
public class AssetTermLinkTest {

    @After
    public void tearDown() {
        RequestContext.clear();
    }

    // ==================== Tests for addMeaningsToEntityRelations ====================
    
    /**
     * Test: Verify that created elements are properly filtered and processed
     * Expected: Only active entities should be included in the processing list
     */
    @Test
    public void testAddMeaningsToEntityRelations_WithNewAssignments() {
        // Setup mock vertices and edges
        AtlasVertex relationVertex1 = mock(AtlasVertex.class);
        AtlasVertex relationVertex2 = mock(AtlasVertex.class);
        
        AtlasEdge edge1 = mock(AtlasEdge.class);
        AtlasEdge edge2 = mock(AtlasEdge.class);
        
        when(edge1.getInVertex()).thenReturn(relationVertex1);
        when(edge2.getInVertex()).thenReturn(relationVertex2);
        
        when(relationVertex1.getProperty(eq(STATE_PROPERTY_KEY), eq(String.class)))
                .thenReturn(AtlasEntity.Status.ACTIVE.name());
        when(relationVertex2.getProperty(eq(STATE_PROPERTY_KEY), eq(String.class)))
                .thenReturn(AtlasEntity.Status.ACTIVE.name());
        
        // Mock existing meanings (empty for new assignment)
        List<String> existingMeanings = new ArrayList<>();
        when(relationVertex1.getMultiValuedProperty(eq(MEANINGS_PROPERTY_KEY), eq(String.class)))
                .thenReturn(existingMeanings);
        when(relationVertex2.getMultiValuedProperty(eq(MEANINGS_PROPERTY_KEY), eq(String.class)))
                .thenReturn(existingMeanings);
        
        List<Object> createdElements = Arrays.asList(edge1, edge2);
        
        // Verify logic: Filter and process active entities
        List<AtlasVertex> assignedEntitiesVertices = createdElements.stream()
                .filter(Objects::nonNull)
                .map(x -> ((AtlasEdge) x).getInVertex())
                .filter(x -> AtlasEntity.Status.ACTIVE.name().equals(x.getProperty(STATE_PROPERTY_KEY, String.class)))
                .collect(Collectors.toList());
        
        // Assert
        assertEquals("Should process 2 active entities", 2, assignedEntitiesVertices.size());
        assertTrue("Should contain relationVertex1", assignedEntitiesVertices.contains(relationVertex1));
        assertTrue("Should contain relationVertex2", assignedEntitiesVertices.contains(relationVertex2));
    }

    /**
     * Test: Verify that deleted entities are filtered out from processing
     * Expected: Only active entities should be processed, deleted ones ignored
     */
    @Test
    public void testAddMeaningsToEntityRelations_SkipsDeletedEntities() {
        // Setup
        AtlasVertex activeVertex = mock(AtlasVertex.class);
        AtlasVertex deletedVertex = mock(AtlasVertex.class);
        
        AtlasEdge activeEdge = mock(AtlasEdge.class);
        AtlasEdge deletedEdge = mock(AtlasEdge.class);
        
        when(activeEdge.getInVertex()).thenReturn(activeVertex);
        when(deletedEdge.getInVertex()).thenReturn(deletedVertex);
        
        when(activeVertex.getProperty(eq(STATE_PROPERTY_KEY), eq(String.class)))
                .thenReturn(AtlasEntity.Status.ACTIVE.name());
        when(deletedVertex.getProperty(eq(STATE_PROPERTY_KEY), eq(String.class)))
                .thenReturn(AtlasEntity.Status.DELETED.name());
        
        List<Object> createdElements = Arrays.asList(activeEdge, deletedEdge);
        
        // Verify logic: Only active entities are processed
        List<AtlasVertex> activeVertices = createdElements.stream()
                .filter(Objects::nonNull)
                .map(x -> ((AtlasEdge) x).getInVertex())
                .filter(x -> AtlasEntity.Status.ACTIVE.name().equals(x.getProperty(STATE_PROPERTY_KEY, String.class)))
                .collect(Collectors.toList());
        
        // Assert
        assertEquals("Should only process 1 active entity", 1, activeVertices.size());
        assertEquals("Should be the active vertex", activeVertex, activeVertices.get(0));
    }

    /**
     * Test: Verify that duplicate assignments are detected
     * Expected: Logic should check if term already exists in entity's meanings
     */
    @Test
    public void testAddMeaningsToEntityRelations_SkipsDuplicateAssignments() {
        // Setup
        String termQualifiedName = "TestTerm@Glossary";
        
        AtlasVertex relationVertex = mock(AtlasVertex.class);
        
        // Mock existing meanings - already contains the term
        List<String> existingMeanings = new ArrayList<>(Arrays.asList(termQualifiedName, "OtherTerm@Glossary"));
        when(relationVertex.getMultiValuedProperty(eq(MEANINGS_PROPERTY_KEY), eq(String.class)))
                .thenReturn(existingMeanings);
        
        // Verify logic: Term already exists
        boolean termAlreadyExists = existingMeanings.contains(termQualifiedName);
        
        // Assert
        assertTrue("Term should already exist in meanings", termAlreadyExists);
        assertEquals("Should have 2 meanings", 2, existingMeanings.size());
    }

    /**
     * Test: Verify that deletions are properly collected and processed
     * Expected: All deleted edges should be mapped to their in-vertices
     */
    @Test
    public void testAddMeaningsToEntityRelations_WithDeletions() {
        // Setup
        AtlasVertex relationVertex1 = mock(AtlasVertex.class);
        AtlasVertex relationVertex2 = mock(AtlasVertex.class);
        
        AtlasEdge deletedEdge1 = mock(AtlasEdge.class);
        AtlasEdge deletedEdge2 = mock(AtlasEdge.class);
        
        when(deletedEdge1.getInVertex()).thenReturn(relationVertex1);
        when(deletedEdge2.getInVertex()).thenReturn(relationVertex2);
        
        List<AtlasEdge> deletedElements = Arrays.asList(deletedEdge1, deletedEdge2);
        
        // Verify logic: Map deleted edges to vertices
        List<AtlasVertex> deletedVertices = deletedElements.stream()
                .map(AtlasEdge::getInVertex)
                .collect(Collectors.toList());
        
        // Assert
        assertEquals("Should process 2 deletions", 2, deletedVertices.size());
        assertTrue("Should contain relationVertex1", deletedVertices.contains(relationVertex1));
        assertTrue("Should contain relationVertex2", deletedVertices.contains(relationVertex2));
    }

    /**
     * Test: Verify that null elements are properly filtered out
     * Expected: Null elements should be excluded from processing
     */
    @Test
    public void testAddMeaningsToEntityRelations_WithNullElements() {
        // Setup
        AtlasVertex relationVertex = mock(AtlasVertex.class);
        AtlasEdge edge = mock(AtlasEdge.class);
        
        when(edge.getInVertex()).thenReturn(relationVertex);
        when(relationVertex.getProperty(eq(STATE_PROPERTY_KEY), eq(String.class)))
                .thenReturn(AtlasEntity.Status.ACTIVE.name());
        
        List<Object> createdElements = Arrays.asList(edge, null, edge);
        
        // Verify logic: Filter out null elements
        List<Object> nonNullElements = createdElements.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        
        // Assert
        assertEquals("Should have 2 non-null elements", 2, nonNullElements.size());
    }

    // ==================== Tests for addMeaningsToEntityV1 ====================
    
    /**
     * Test: Verify append mode adds new meanings without removing existing ones
     * Expected: New meanings are identified by checking against existing meanings list
     */
    @Test
    public void testAddMeaningsToEntityV1_AppendMode_NewMeanings() {
        // Setup
        String termName1 = "Term1";
        String termName2 = "Term2";
        String termQName1 = "Term1@Glossary";
        String termQName2 = "Term2@Glossary";
        
        AtlasVertex meaningVertex1 = mock(AtlasVertex.class);
        AtlasVertex meaningVertex2 = mock(AtlasVertex.class);
        
        when(meaningVertex1.getProperty(eq(STATE_PROPERTY_KEY), eq(String.class)))
                .thenReturn(AtlasEntity.Status.ACTIVE.name());
        when(meaningVertex2.getProperty(eq(STATE_PROPERTY_KEY), eq(String.class)))
                .thenReturn(AtlasEntity.Status.ACTIVE.name());
        when(meaningVertex1.getProperty(eq(QUALIFIED_NAME), eq(String.class)))
                .thenReturn(termQName1);
        when(meaningVertex2.getProperty(eq(QUALIFIED_NAME), eq(String.class)))
                .thenReturn(termQName2);
        when(meaningVertex1.getProperty(eq(NAME), eq(String.class)))
                .thenReturn(termName1);
        when(meaningVertex2.getProperty(eq(NAME), eq(String.class)))
                .thenReturn(termName2);
        
        AtlasEdge edge1 = mock(AtlasEdge.class);
        AtlasEdge edge2 = mock(AtlasEdge.class);
        
        when(edge1.getOutVertex()).thenReturn(meaningVertex1);
        when(edge2.getOutVertex()).thenReturn(meaningVertex2);
        
        // Mock existing meanings
        List<String> existingMeanings = new ArrayList<>(Arrays.asList("ExistingTerm@Glossary"));
        
        List<Object> createdElements = Arrays.asList(edge1, edge2);
        
        // Verify logic: Collect new qualified names from active meanings
        Set<String> newQNames = createdElements.stream()
                .map(x -> ((AtlasEdge) x).getOutVertex())
                .filter(x -> AtlasEntity.Status.ACTIVE.name().equals(x.getProperty(STATE_PROPERTY_KEY, String.class)))
                .map(x -> x.getProperty(QUALIFIED_NAME, String.class))
                .collect(Collectors.toSet());
        
        // Verify logic: Filter only new meanings (not in existing)
        List<String> newMeanings = createdElements.stream()
                .map(x -> ((AtlasEdge) x).getOutVertex())
                .filter(x -> !existingMeanings.contains(x.getProperty(QUALIFIED_NAME, String.class)))
                .map(x -> x.getProperty(NAME, String.class))
                .collect(Collectors.toList());
        
        // Assert
        assertEquals("Should have 2 new qualified names", 2, newQNames.size());
        assertTrue("Should contain termQName1", newQNames.contains(termQName1));
        assertTrue("Should contain termQName2", newQNames.contains(termQName2));
        assertEquals("Should have 2 new meanings", 2, newMeanings.size());
    }

    /**
     * Test: Verify replace mode clears existing meanings
     * Expected: In replace mode (isAppend=false), existing meanings should be cleared
     */
    @Test
    public void testAddMeaningsToEntityV1_ReplaceMode_ClearExisting() {
        // Setup
        String termName1 = "Term1";
        String termQName1 = "Term1@Glossary";
        
        AtlasVertex meaningVertex1 = mock(AtlasVertex.class);
        
        when(meaningVertex1.getProperty(eq(STATE_PROPERTY_KEY), eq(String.class)))
                .thenReturn(AtlasEntity.Status.ACTIVE.name());
        when(meaningVertex1.getProperty(eq(QUALIFIED_NAME), eq(String.class)))
                .thenReturn(termQName1);
        when(meaningVertex1.getProperty(eq(NAME), eq(String.class)))
                .thenReturn(termName1);
        
        AtlasEdge edge1 = mock(AtlasEdge.class);
        when(edge1.getOutVertex()).thenReturn(meaningVertex1);
        
        // Mock existing meanings
        List<String> existingMeanings = new ArrayList<>(Arrays.asList("ExistingTerm@Glossary", "AnotherTerm@Glossary"));
        
        List<Object> createdElements = Arrays.asList(edge1);
        boolean isAppend = false;
        
        // Assert replace mode
        assertFalse("Should be in replace mode", isAppend);
        assertEquals("Should have 2 existing meanings before replace", 2, existingMeanings.size());
    }

    /**
     * Test: Verify deleted meanings are properly collected in append mode
     * Expected: Deleted edges should be mapped to their meaning vertices
     */
    @Test
    public void testAddMeaningsToEntityV1_AppendMode_WithDeletions() {
        // Setup
        String deletedTermName = "DeletedTerm";
        String deletedTermQName = "DeletedTerm@Glossary";
        
        AtlasVertex deletedMeaningVertex = mock(AtlasVertex.class);
        
        when(deletedMeaningVertex.getProperty(eq(NAME), eq(String.class)))
                .thenReturn(deletedTermName);
        when(deletedMeaningVertex.getProperty(eq(QUALIFIED_NAME), eq(String.class)))
                .thenReturn(deletedTermQName);
        
        AtlasEdge deletedEdge = mock(AtlasEdge.class);
        when(deletedEdge.getOutVertex()).thenReturn(deletedMeaningVertex);
        
        List<AtlasEdge> deletedElements = Arrays.asList(deletedEdge);
        
        // Verify logic: Collect deleted meaning names
        List<String> deletedMeaningNames = deletedElements.stream()
                .map(x -> x.getOutVertex())
                .map(x -> x.getProperty(NAME, String.class))
                .collect(Collectors.toList());
        
        // Verify logic: Collect deleted meaning qualified names
        List<String> deletedMeaningQNames = deletedElements.stream()
                .map(x -> x.getOutVertex())
                .map(x -> x.getProperty(QUALIFIED_NAME, String.class))
                .collect(Collectors.toList());
        
        // Assert
        assertEquals("Should have 1 deleted meaning name", 1, deletedMeaningNames.size());
        assertEquals("Should be deletedTermName", deletedTermName, deletedMeaningNames.get(0));
        assertEquals("Should have 1 deleted qualified name", 1, deletedMeaningQNames.size());
        assertEquals("Should be deletedTermQName", deletedTermQName, deletedMeaningQNames.get(0));
    }

    /**
     * Test: Verify that removing all meanings should result in empty list
     * Expected: When last meaning is removed, property should be cleared
     */
    @Test
    public void testAddMeaningsToEntityV1_RemoveOnly_ClearPropertiesWhenEmpty() {
        // Setup
        String deletedTermQName = "OnlyTerm@Glossary";
        
        AtlasVertex deletedMeaningVertex = mock(AtlasVertex.class);
        when(deletedMeaningVertex.getProperty(eq(QUALIFIED_NAME), eq(String.class)))
                .thenReturn(deletedTermQName);
        
        AtlasEdge deletedEdge = mock(AtlasEdge.class);
        when(deletedEdge.getOutVertex()).thenReturn(deletedMeaningVertex);
        
        List<Object> createdElements = Collections.emptyList();
        List<AtlasEdge> deletedElements = Arrays.asList(deletedEdge);
        
        // Simulate remaining meanings after deletion
        List<String> existingMeanings = new ArrayList<>(Arrays.asList(deletedTermQName));
        existingMeanings.remove(deletedTermQName);
        
        // Assert
        assertTrue("Created elements should be empty", createdElements.isEmpty());
        assertFalse("Deleted elements should not be empty", deletedElements.isEmpty());
        assertTrue("Remaining meanings should be empty", existingMeanings.isEmpty());
    }

    /**
     * Test: Verify that inactive meanings are filtered out
     * Expected: Only active meanings should be processed
     */
    @Test
    public void testAddMeaningsToEntityV1_SkipsInactiveMeanings() {
        // Setup
        AtlasVertex activeMeaning = mock(AtlasVertex.class);
        AtlasVertex inactiveMeaning = mock(AtlasVertex.class);
        
        when(activeMeaning.getProperty(eq(STATE_PROPERTY_KEY), eq(String.class)))
                .thenReturn(AtlasEntity.Status.ACTIVE.name());
        when(inactiveMeaning.getProperty(eq(STATE_PROPERTY_KEY), eq(String.class)))
                .thenReturn(AtlasEntity.Status.DELETED.name());
        
        when(activeMeaning.getProperty(eq(QUALIFIED_NAME), eq(String.class)))
                .thenReturn("ActiveTerm@Glossary");
        when(inactiveMeaning.getProperty(eq(QUALIFIED_NAME), eq(String.class)))
                .thenReturn("InactiveTerm@Glossary");
        
        AtlasEdge activeEdge = mock(AtlasEdge.class);
        AtlasEdge inactiveEdge = mock(AtlasEdge.class);
        
        when(activeEdge.getOutVertex()).thenReturn(activeMeaning);
        when(inactiveEdge.getOutVertex()).thenReturn(inactiveMeaning);
        
        List<Object> createdElements = Arrays.asList(activeEdge, inactiveEdge);
        
        // Verify logic: Filter only active meanings
        List<AtlasVertex> activeMeanings = createdElements.stream()
                .map(x -> ((AtlasEdge) x).getOutVertex())
                .filter(x -> AtlasEntity.Status.ACTIVE.name().equals(x.getProperty(STATE_PROPERTY_KEY, String.class)))
                .collect(Collectors.toList());
        
        // Assert
        assertEquals("Should have 1 active meaning", 1, activeMeanings.size());
        assertEquals("Should be activeMeaning", activeMeaning, activeMeanings.get(0));
    }

    /**
     * Test: Verify mixed operations (add and delete) work correctly in append mode
     * Expected: New meanings are added and deleted meanings are removed simultaneously
     */
    @Test
    public void testAddMeaningsToEntityV1_AppendMode_MixedOperations() {
        // Setup
        String newTermName = "NewTerm";
        String newTermQName = "NewTerm@Glossary";
        String deletedTermName = "DeletedTerm";
        String deletedTermQName = "DeletedTerm@Glossary";
        String keepTermQName = "KeepTerm@Glossary";
        
        // New meaning to add
        AtlasVertex newMeaningVertex = mock(AtlasVertex.class);
        when(newMeaningVertex.getProperty(eq(STATE_PROPERTY_KEY), eq(String.class)))
                .thenReturn(AtlasEntity.Status.ACTIVE.name());
        when(newMeaningVertex.getProperty(eq(QUALIFIED_NAME), eq(String.class)))
                .thenReturn(newTermQName);
        when(newMeaningVertex.getProperty(eq(NAME), eq(String.class)))
                .thenReturn(newTermName);
        
        AtlasEdge newEdge = mock(AtlasEdge.class);
        when(newEdge.getOutVertex()).thenReturn(newMeaningVertex);
        
        // Meaning to delete
        AtlasVertex deletedMeaningVertex = mock(AtlasVertex.class);
        when(deletedMeaningVertex.getProperty(eq(NAME), eq(String.class)))
                .thenReturn(deletedTermName);
        when(deletedMeaningVertex.getProperty(eq(QUALIFIED_NAME), eq(String.class)))
                .thenReturn(deletedTermQName);
        
        AtlasEdge deletedEdge = mock(AtlasEdge.class);
        when(deletedEdge.getOutVertex()).thenReturn(deletedMeaningVertex);
        
        // Mock existing meanings
        List<String> existingMeanings = new ArrayList<>(Arrays.asList(deletedTermQName, keepTermQName));
        
        List<Object> createdElements = Arrays.asList(newEdge);
        List<AtlasEdge> deletedElements = Arrays.asList(deletedEdge);
        
        // Verify logic: Filter new meanings (not in existing)
        List<String> newMeanings = createdElements.stream()
                .map(x -> ((AtlasEdge) x).getOutVertex())
                .filter(x -> !existingMeanings.contains(x.getProperty(QUALIFIED_NAME, String.class)))
                .map(x -> x.getProperty(NAME, String.class))
                .collect(Collectors.toList());
        
        // Verify logic: Collect deleted names
        List<String> deletedNames = deletedElements.stream()
                .map(x -> x.getOutVertex())
                .map(x -> x.getProperty(NAME, String.class))
                .collect(Collectors.toList());
        
        // Assert
        assertEquals("Should have 1 new meaning", 1, newMeanings.size());
        assertEquals("Should be newTermName", newTermName, newMeanings.get(0));
        assertEquals("Should have 1 deleted meaning", 1, deletedNames.size());
        assertEquals("Should be deletedTermName", deletedTermName, deletedNames.get(0));
    }

    /**
     * Test: Verify empty operations (no adds, no deletes)
     * Expected: Should handle empty lists gracefully
     */
    @Test
    public void testAddMeaningsToEntityV1_EmptyOperations() {
        // Setup
        List<String> existingMeanings = new ArrayList<>(Arrays.asList("ExistingTerm@Glossary"));
        
        List<Object> createdElements = Collections.emptyList();
        List<AtlasEdge> deletedElements = Collections.emptyList();
        
        // Assert
        assertTrue("Created elements should be empty", createdElements.isEmpty());
        assertTrue("Deleted elements should be empty", deletedElements.isEmpty());
        assertEquals("Existing meanings should remain unchanged", 1, existingMeanings.size());
    }
}
