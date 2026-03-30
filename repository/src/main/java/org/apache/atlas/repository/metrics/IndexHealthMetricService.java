/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasBusinessMetadataType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

/**
 * Audits JanusGraph mixed index health and exposes Prometheus metrics.
 *
 * For every indexable attribute (primitives, enums, arrays of primitives/enums),
 * checks whether a property key is registered in the mixed index.
 * Missing property keys mean JanusGraph will never sync that attribute to ES.
 *
 * Uses a single set of AtomicInteger fields that are registered once and updated
 * on each audit call. This avoids Micrometer's stale reference problem.
 */
@Service
public class IndexHealthMetricService {
    private static final Logger LOG = LoggerFactory.getLogger(IndexHealthMetricService.class);

    private static final String METRIC_PREFIX = "atlas_index_health";

    private static final Set<String> CORE_TYPE_NAMES = Set.of(
            "Referenceable", "Asset", "DataSet", "Process", "Infrastructure",
            "DataDomain", "DataProduct", "Connection",
            "AuthPolicy", "Persona", "Purpose", "AccessControl", "StakeholderTitle",
            "AtlasGlossary", "AtlasGlossaryTerm", "AtlasGlossaryCategory",
            "AuthService", "AtlasServer",
            "Table", "Column", "Schema", "Database", "View", "MaterialisedView",
            "Query", "Folder", "Collection"
    );

    private final MeterRegistry meterRegistry;
    private final AtlasTypeRegistry typeRegistry;
    private final String tenant;

    // Single set of AtomicIntegers — registered once, updated on each audit
    private final AtomicInteger entityExpected = new AtomicInteger(0);
    private final AtomicInteger entityIndexed = new AtomicInteger(0);
    private final AtomicInteger entityMissing = new AtomicInteger(0);
    private final AtomicInteger entityCoreMissing = new AtomicInteger(0);
    private final AtomicInteger entityCustomMissing = new AtomicInteger(0);
    private final AtomicInteger bmExpected = new AtomicInteger(0);
    private final AtomicInteger bmIndexed = new AtomicInteger(0);
    private final AtomicInteger bmMissing = new AtomicInteger(0);
    private final AtomicInteger coreMissingTotal = new AtomicInteger(0);
    private final AtomicInteger healthStatus = new AtomicInteger(1);
    // Bounded cardinality: one entry per entity type (~100-500 per tenant, not user-generated)
    private final Map<String, AtomicInteger> perTypeMissing = new HashMap<>();
    private boolean gaugesRegistered = false;
    private volatile long lastAuditTimeMs = 0;
    private static final long MIN_AUDIT_INTERVAL_MS = 60_000; // 1 minute throttle

    @Inject
    public IndexHealthMetricService(AtlasTypeRegistry typeRegistry) {
        this(typeRegistry, getMeterRegistry());
    }

    IndexHealthMetricService(AtlasTypeRegistry typeRegistry, MeterRegistry meterRegistry) {
        this.typeRegistry = typeRegistry;
        this.meterRegistry = meterRegistry;
        String domainName = System.getenv("DOMAIN_NAME");
        this.tenant = (domainName != null) ? domainName : "default";
    }

    /**
     * Register gauges only once, using the same AtomicInteger references forever.
     * Called on first audit — not in constructor — to avoid registration before
     * the singleton is established.
     */
    private synchronized void registerGaugesOnce() {
        if (gaugesRegistered) {
            return;
        }

        Tags tenantTag = Tags.of("tenant", tenant);

        Gauge.builder(METRIC_PREFIX + "_expected_total", entityExpected, AtomicInteger::get)
                .description("Total indexable attributes expected in mixed index")
                .tags(tenantTag).tag("category", "entity").register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_indexed_total", entityIndexed, AtomicInteger::get)
                .description("Attributes registered in mixed index")
                .tags(tenantTag).tag("category", "entity").register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_missing_total", entityMissing, AtomicInteger::get)
                .description("Attributes missing from mixed index")
                .tags(tenantTag).tag("category", "entity").tag("source", "all").register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_missing_total", entityCoreMissing, AtomicInteger::get)
                .description("Core type attributes missing")
                .tags(tenantTag).tag("category", "entity").tag("source", "core").register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_missing_total", entityCustomMissing, AtomicInteger::get)
                .description("Custom type attributes missing")
                .tags(tenantTag).tag("category", "entity").tag("source", "custom").register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_expected_total", bmExpected, AtomicInteger::get)
                .description("Total BM attributes expected")
                .tags(tenantTag).tag("category", "business_metadata").register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_indexed_total", bmIndexed, AtomicInteger::get)
                .description("BM attributes registered")
                .tags(tenantTag).tag("category", "business_metadata").register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_missing_total", bmMissing, AtomicInteger::get)
                .description("BM attributes missing")
                .tags(tenantTag).tag("category", "business_metadata").tag("source", "all").register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_core_missing_total", coreMissingTotal, AtomicInteger::get)
                .description("Core type attributes missing — affects all tenants if > 0")
                .tags(tenantTag).register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_status", healthStatus, AtomicInteger::get)
                .description("Overall index health: 1 = healthy, 0 = unhealthy")
                .tags(tenantTag).register(meterRegistry);

        gaugesRegistered = true;
    }

    public void auditIndexHealth(AtlasGraphManagement managementSystem) {
        if (managementSystem == null) {
            LOG.warn("Cannot audit index health: managementSystem is null");
            return;
        }

        // Throttle: skip if last audit was less than 1 minute ago (avoid overhead on rapid typedef changes)
        long now = System.currentTimeMillis();
        if (lastAuditTimeMs > 0 && (now - lastAuditTimeMs) < MIN_AUDIT_INTERVAL_MS) {
            LOG.debug("Skipping index health audit — last audit was {}ms ago (throttle: {}ms)",
                    now - lastAuditTimeMs, MIN_AUDIT_INTERVAL_MS);
            return;
        }
        lastAuditTimeMs = now;

        LOG.info("Starting index health audit...");

        // Register gauges on first call — same AtomicInteger references used forever
        registerGaugesOnce();

        int totalExpected = 0, totalIndexed = 0, totalMissing = 0;
        int totalCoreMissing = 0, totalCustomMissing = 0;
        int totalBmExpected = 0, totalBmIndexed = 0, totalBmMissing = 0;
        List<String> missingAttributes = new ArrayList<>();

        // Get the mixed index field keys — this is what determines if an attribute is searchable in ES
        AtlasGraphIndex vertexIndex = managementSystem.getGraphIndex("vertex_index");
        if (vertexIndex == null) {
            LOG.error("INDEX HEALTH AUDIT: vertex_index not found in graph management. "
                    + "Mixed index may not be initialized. Reporting UNHEALTHY.");
            healthStatus.set(0);
            return;
        }
        Set<AtlasPropertyKey> indexedFieldKeys = new HashSet<>(vertexIndex.getFieldKeys());

        // Audit entity types
        for (AtlasEntityType entityType : typeRegistry.getAllEntityDefs()
                .stream()
                .map(def -> typeRegistry.getEntityTypeByName(def.getName()))
                .filter(Objects::nonNull)
                .toList()) {

            String typeName = entityType.getTypeName();
            boolean isCoreType = CORE_TYPE_NAMES.contains(typeName);
            int typeMissingCount = 0;

            for (AtlasAttribute attribute : entityType.getAllAttributes().values()) {
                if (!isIndexedInMixedIndex(attribute)) {
                    continue;
                }
                if (!attribute.getAttributeDef().getIsIndexable()) {
                    continue;
                }

                totalExpected++;

                // Check mixed index registration — not just property key existence.
                // A property key can exist in the schema but NOT be in the vertex_index.
                AtlasPropertyKey propertyKey = managementSystem.getPropertyKey(attribute.getVertexPropertyName());
                if (propertyKey != null && indexedFieldKeys.contains(propertyKey)) {
                    totalIndexed++;
                } else {
                    totalMissing++;
                    typeMissingCount++;
                    if (isCoreType) {
                        totalCoreMissing++;
                    } else {
                        totalCustomMissing++;
                    }
                    missingAttributes.add(typeName + "." + attribute.getName());
                }
            }

            // Always update per-type gauge — reset to 0 if type is now fully indexed
            getOrCreatePerTypeGauge(typeName).set(typeMissingCount);
        }

        // Audit business metadata types
        for (AtlasBusinessMetadataType bmType : typeRegistry.getAllBusinessMetadataDefs()
                .stream()
                .map(def -> typeRegistry.getBusinessMetadataTypeByName(def.getName()))
                .filter(Objects::nonNull)
                .toList()) {

            for (AtlasAttribute attribute : bmType.getAllAttributes().values()) {
                if (!isIndexedInMixedIndex(attribute)) {
                    continue;
                }
                if (!attribute.getAttributeDef().getIsIndexable()) {
                    continue;
                }

                totalBmExpected++;

                AtlasPropertyKey propertyKey = managementSystem.getPropertyKey(attribute.getVertexPropertyName());
                if (propertyKey != null && indexedFieldKeys.contains(propertyKey)) {
                    totalBmIndexed++;
                } else {
                    totalBmMissing++;
                    missingAttributes.add(bmType.getTypeName() + "." + attribute.getName());
                }
            }
        }

        // Update gauge values — same AtomicInteger objects the gauges are bound to
        entityExpected.set(totalExpected);
        entityIndexed.set(totalIndexed);
        entityMissing.set(totalMissing);
        entityCoreMissing.set(totalCoreMissing);
        entityCustomMissing.set(totalCustomMissing);
        bmExpected.set(totalBmExpected);
        bmIndexed.set(totalBmIndexed);
        bmMissing.set(totalBmMissing);
        coreMissingTotal.set(totalCoreMissing);

        boolean isHealthy = (totalMissing + totalBmMissing) == 0;

        // Guard against false positive: if type registry is empty/incomplete,
        // expected=0 would make it look healthy when it's actually broken
        if (totalExpected == 0) {
            LOG.warn("INDEX HEALTH AUDIT: 0 entity attributes found — type registry may be empty. Reporting UNHEALTHY.");
            isHealthy = false;
        }

        healthStatus.set(isHealthy ? 1 : 0);

        if (isHealthy) {
            LOG.info("Index health audit PASSED: {}/{} entity attrs indexed, {}/{} BM attrs indexed",
                    totalIndexed, totalExpected, totalBmIndexed, totalBmExpected);
        } else {
            LOG.error("INDEX HEALTH AUDIT FAILED: {} entity attrs missing (core={}, custom={}), {} BM attrs missing. Missing: {}",
                    totalMissing, totalCoreMissing, totalCustomMissing, totalBmMissing, missingAttributes);
        }
    }

    /**
     * Checks if an attribute is the type that Cedar indexes into the JanusGraph mixed index.
     * Only these types get property keys registered in vertex_index:
     * - Primitive types (string, int, long, boolean, etc.)
     * - Enum types (indexed as String)
     * - Array of primitive types (e.g., array<string>)
     * - Array of enum types
     *
     * Map, struct, entity, relationship, and classification types are NOT indexed.
     */
    private boolean isIndexedInMixedIndex(AtlasAttribute attribute) {
        TypeCategory typeCategory = attribute.getAttributeType().getTypeCategory();

        if (TypeCategory.PRIMITIVE.equals(typeCategory) || TypeCategory.ENUM.equals(typeCategory)) {
            return true;
        }

        if (TypeCategory.ARRAY.equals(typeCategory)) {
            AtlasType elementType = ((org.apache.atlas.type.AtlasArrayType) attribute.getAttributeType()).getElementType();
            TypeCategory elementCategory = elementType.getTypeCategory();
            return TypeCategory.PRIMITIVE.equals(elementCategory) || TypeCategory.ENUM.equals(elementCategory);
        }

        return false;
    }

    private AtomicInteger getOrCreatePerTypeGauge(String typeName) {
        return perTypeMissing.computeIfAbsent(typeName, name -> {
            AtomicInteger counter = new AtomicInteger(0);
            Gauge.builder(METRIC_PREFIX + "_type_missing", counter, AtomicInteger::get)
                    .description("Missing attributes for entity type")
                    .tag("typeName", name)
                    .tag("tenant", tenant)
                    .register(meterRegistry);
            return counter;
        });
    }
}
