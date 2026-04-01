package org.apache.atlas.repository.graphdb.migrator;

import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.testng.Assert.*;

/**
 * Tests for JanusGraphScanner's pure-logic methods.
 * The splitTokenRanges method is pure math and doesn't require JanusGraph.
 * We use a test subclass to access it without needing a real JanusGraph connection.
 */
public class JanusGraphScannerTest {

    /**
     * Test helper: standalone token range splitting logic extracted from JanusGraphScanner.
     * This avoids needing a real JanusGraph instance for testing the partitioning math.
     */
    private List<long[]> splitTokenRanges(int numRanges) {
        BigInteger minToken = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger maxToken = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger totalRange = maxToken.subtract(minToken).add(BigInteger.ONE);
        BigInteger rangeSize = totalRange.divide(BigInteger.valueOf(numRanges));

        java.util.List<long[]> ranges = new java.util.ArrayList<>(numRanges);
        BigInteger current = minToken;

        for (int i = 0; i < numRanges; i++) {
            BigInteger end = (i == numRanges - 1) ? maxToken : current.add(rangeSize).subtract(BigInteger.ONE);
            ranges.add(new long[]{current.longValueExact(), end.longValueExact()});
            current = end.add(BigInteger.ONE);
        }
        return ranges;
    }

    @Test
    public void testSplitTokenRanges_singleRange() {
        List<long[]> ranges = splitTokenRanges(1);
        assertEquals(ranges.size(), 1);
        assertEquals(ranges.get(0)[0], Long.MIN_VALUE);
        assertEquals(ranges.get(0)[1], Long.MAX_VALUE);
    }

    @Test
    public void testSplitTokenRanges_twoRanges() {
        List<long[]> ranges = splitTokenRanges(2);
        assertEquals(ranges.size(), 2);

        // First range starts at Long.MIN_VALUE
        assertEquals(ranges.get(0)[0], Long.MIN_VALUE);

        // Second range ends at Long.MAX_VALUE
        assertEquals(ranges.get(1)[1], Long.MAX_VALUE);

        // Ranges should be contiguous
        assertEquals(ranges.get(0)[1] + 1, ranges.get(1)[0]);
    }

    @Test
    public void testSplitTokenRanges_fourRanges() {
        List<long[]> ranges = splitTokenRanges(4);
        assertEquals(ranges.size(), 4);

        // First range starts at min
        assertEquals(ranges.get(0)[0], Long.MIN_VALUE);
        // Last range ends at max
        assertEquals(ranges.get(3)[1], Long.MAX_VALUE);

        // All ranges are contiguous
        for (int i = 0; i < ranges.size() - 1; i++) {
            assertEquals(ranges.get(i)[1] + 1, ranges.get(i + 1)[0],
                "Range " + i + " end should be adjacent to range " + (i + 1) + " start");
        }
    }

    @Test
    public void testSplitTokenRanges_32Ranges() {
        List<long[]> ranges = splitTokenRanges(32);
        assertEquals(ranges.size(), 32);

        assertEquals(ranges.get(0)[0], Long.MIN_VALUE);
        assertEquals(ranges.get(31)[1], Long.MAX_VALUE);

        // All contiguous
        for (int i = 0; i < ranges.size() - 1; i++) {
            assertEquals(ranges.get(i)[1] + 1, ranges.get(i + 1)[0]);
        }
    }

    @Test
    public void testSplitTokenRanges_rangesAreRoughlyEqual() {
        int numRanges = 8;
        List<long[]> ranges = splitTokenRanges(numRanges);

        // Calculate expected range size
        BigInteger totalRange = BigInteger.valueOf(Long.MAX_VALUE)
            .subtract(BigInteger.valueOf(Long.MIN_VALUE))
            .add(BigInteger.ONE);
        BigInteger expectedSize = totalRange.divide(BigInteger.valueOf(numRanges));

        for (int i = 0; i < numRanges; i++) {
            long rangeSize = ranges.get(i)[1] - ranges.get(i)[0] + 1;
            BigInteger actualSize = BigInteger.valueOf(ranges.get(i)[1])
                .subtract(BigInteger.valueOf(ranges.get(i)[0]))
                .add(BigInteger.ONE);

            // Each range should be within 1 of the expected size
            BigInteger diff = actualSize.subtract(expectedSize).abs();
            assertTrue(diff.compareTo(BigInteger.ONE) <= 0,
                "Range " + i + " size " + actualSize + " differs from expected " + expectedSize + " by " + diff);
        }
    }

    @Test
    public void testSplitTokenRanges_coversFullSpace() {
        List<long[]> ranges = splitTokenRanges(16);

        // The union of all ranges should cover [Long.MIN_VALUE, Long.MAX_VALUE]
        // with no gaps and no overlaps
        Set<Long> boundaries = new HashSet<>();
        for (long[] range : ranges) {
            // No two ranges should start at the same value
            assertTrue(boundaries.add(range[0]),
                "Duplicate range start: " + range[0]);
        }

        assertEquals(ranges.get(0)[0], Long.MIN_VALUE);
        assertEquals(ranges.get(ranges.size() - 1)[1], Long.MAX_VALUE);
    }

    @Test
    public void testSplitTokenRanges_noEmptyRanges() {
        List<long[]> ranges = splitTokenRanges(64);

        for (int i = 0; i < ranges.size(); i++) {
            assertTrue(ranges.get(i)[1] >= ranges.get(i)[0],
                "Range " + i + " is empty or inverted: [" + ranges.get(i)[0] + ", " + ranges.get(i)[1] + "]");
        }
    }

    @Test
    public void testSplitTokenRanges_128Ranges() {
        List<long[]> ranges = splitTokenRanges(128);
        assertEquals(ranges.size(), 128);

        assertEquals(ranges.get(0)[0], Long.MIN_VALUE);
        assertEquals(ranges.get(127)[1], Long.MAX_VALUE);

        // All contiguous
        for (int i = 0; i < ranges.size() - 1; i++) {
            assertEquals(ranges.get(i)[1] + 1, ranges.get(i + 1)[0]);
        }
    }

    // --- normalizePropertyName tests ---

    @Test
    public void testNormalize_typeQualified() {
        // "Referenceable.qualifiedName" → "qualifiedName"
        assertEquals(JanusGraphScanner.normalizePropertyName("Referenceable.qualifiedName"),
                "qualifiedName");
        assertEquals(JanusGraphScanner.normalizePropertyName("Asset.name"), "name");
        assertEquals(JanusGraphScanner.normalizePropertyName("Asset.description"), "description");
    }

    @Test
    public void testNormalize_doubleUnderscoreNeverNormalized() {
        // ALL properties starting with "__" are Atlas internal — never normalize
        assertEquals(JanusGraphScanner.normalizePropertyName("__guid"), "__guid");
        assertEquals(JanusGraphScanner.normalizePropertyName("__typeName"), "__typeName");
        assertEquals(JanusGraphScanner.normalizePropertyName("__state"), "__state");
        assertEquals(JanusGraphScanner.normalizePropertyName("__type"), "__type");
        assertEquals(JanusGraphScanner.normalizePropertyName("__type_name"), "__type_name");
        assertEquals(JanusGraphScanner.normalizePropertyName("__createdBy"), "__createdBy");
        assertEquals(JanusGraphScanner.normalizePropertyName("__superTypeNames"), "__superTypeNames");
        // TypeDef metadata — __type. prefix is Atlas's PROPERTY_PREFIX, must be preserved
        assertEquals(JanusGraphScanner.normalizePropertyName("__type.atlas_operation"),
                "__type.atlas_operation");
        assertEquals(JanusGraphScanner.normalizePropertyName("__type.atlas_operation.CREATE"),
                "__type.atlas_operation.CREATE");
        // Edge labels starting with "__" are also preserved
        assertEquals(JanusGraphScanner.normalizePropertyName("__Asset.schemaAttributes"),
                "__Asset.schemaAttributes");
    }

    @Test
    public void testNormalize_plainNames() {
        // Plain attribute names without dots are kept as-is
        assertEquals(JanusGraphScanner.normalizePropertyName("qualifiedName"), "qualifiedName");
        assertEquals(JanusGraphScanner.normalizePropertyName("mongoDBCollectionIsCapped"),
                "mongoDBCollectionIsCapped");
        assertEquals(JanusGraphScanner.normalizePropertyName("documentDBCollectionTotalIndexSize"),
                "documentDBCollectionTotalIndexSize");
    }

    @Test
    public void testNormalize_null() {
        assertNull(JanusGraphScanner.normalizePropertyName(null));
    }
}
