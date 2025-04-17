package org.apache.atlas.repository.graphdb.janus.customdatabase;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.util.HashingUtil;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.InternalAttributeUtil;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
// Import other necessary classes

public class CustomIndexSerializer extends IndexSerializer {

    private static final int DEFAULT_OBJECT_BYTELEN = 30;
    private static final byte FIRST_INDEX_COLUMN_BYTE = 0;

    private final boolean hashKeys;
    private final Serializer serializer;
    private final Configuration configuration;

    private final HashingUtil.HashLength hashLength = HashingUtil.HashLength.SHORT;

    public CustomIndexSerializer(StandardJanusGraph graph) {

        super(graph.getConfiguration().getConfiguration(),
                graph.getConfiguration().getSerializer(),
                graph.getBackend().getIndexInformation(),
                graph.getBackend().getStoreFeatures().isDistributed() && graph.getBackend().getStoreFeatures().isKeyOrdered());

        this.configuration = graph.getConfiguration().getConfiguration();
        this.serializer = graph.getConfiguration().getSerializer();

        this.hashKeys = graph.getBackend().getStoreFeatures().isDistributed() && graph.getBackend().getStoreFeatures().isKeyOrdered();
    }

    private enum Type { ADD, DELETE }

   /* public Collection<IndexUpdate> getIndexUpdates(InternalVertex vertex, Collection<InternalRelation> updatedProperties) {
        if (updatedProperties.isEmpty()) return Collections.emptyList();
        final Set<IndexUpdate> updates = Sets.newHashSet();

        for (final InternalRelation rel : updatedProperties) {
            assert rel.isProperty();
            final JanusGraphVertexProperty p = (JanusGraphVertexProperty)rel;
            assert rel.isNew() || rel.isRemoved(); assert rel.getVertex(0).equals(vertex);
            final Type updateType = getUpdateType(rel);
            for (final IndexType index : ((InternalRelationType)p.propertyKey()).getKeyIndexes()) {
                if (!indexAppliesTo(index,vertex)) continue;
                if (index.isCompositeIndex()) { //Gather composite indexes
                    final CompositeIndexType cIndex = (CompositeIndexType)index;
                    final IndexRecords updateRecords = indexMatches(vertex,cIndex,updateType==Type.DELETE,p.propertyKey(),new RecordEntry(p));
                    for (final RecordEntry[] record : updateRecords) {
                        final IndexUpdate update = new IndexUpdate<>(cIndex, updateType, getIndexKey(cIndex, record), getIndexEntry(cIndex, record, vertex), vertex);
                        final int ttl = getIndexTTL(vertex,getKeysOfRecords(record));
                        if (ttl>0 && updateType== IndexUpdate.Type.ADD) update.setTTL(ttl);
                        updates.add(update);
                    }
                } else { //Update mixed indexes
                    ParameterIndexField field = ((MixedIndexType)index).getField(p.propertyKey());
                    if (field == null) {
                        throw new SchemaViolationException(p.propertyKey() + " is not available in mixed index " + index);
                    }
                    if (field.getStatus() == SchemaStatus.DISABLED) continue;
                    final IndexUpdate update = getMixedIndexUpdate(vertex, p.propertyKey(), p.value(), (MixedIndexType) index, updateType);
                    final int ttl = getIndexTTL(vertex,p.propertyKey());
                    if (ttl>0 && updateType == Type.ADD) update.setTTL(ttl);
                    updates.add(update);
                }
            }
        }
        return updates;
    }*/

    private static Type getUpdateType(InternalRelation relation) {
        assert relation.isNew() || relation.isRemoved();
        return (relation.isNew()? Type.ADD : Type.DELETE);
    }

    private static boolean indexAppliesTo(IndexType index, JanusGraphElement element) {
        return index.getElement().isInstance(element) &&
                (!(index instanceof CompositeIndexType) || ((CompositeIndexType)index).getStatus()!=SchemaStatus.DISABLED) &&
                (!index.hasSchemaTypeConstraint() ||
                        index.getElement().matchesConstraint(index.getSchemaTypeConstraint(),element));
    }

    private static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index,
                                             boolean onlyLoaded, PropertyKey replaceKey, RecordEntry replaceValue) {
        final IndexRecords matches = new IndexRecords();
        final IndexField[] fields = index.getFieldKeys();
        indexMatches(vertex,new RecordEntry[fields.length],matches,fields,0,onlyLoaded,replaceKey,replaceValue);
        return matches;
    }

    private static void indexMatches(JanusGraphVertex vertex, RecordEntry[] current, IndexRecords matches,
                                     IndexField[] fields, int pos,
                                     boolean onlyLoaded, PropertyKey replaceKey, RecordEntry replaceValue) {
        if (pos>= fields.length) {
            matches.add(current);
            return;
        }

        final PropertyKey key = fields[pos].getFieldKey();

        List<RecordEntry> values;
        if (key.equals(replaceKey)) {
            values = ImmutableList.of(replaceValue);
        } else {
            values = new ArrayList<>();
            Iterable<JanusGraphVertexProperty> props;
            if (onlyLoaded ||
                    (!vertex.isNew() && IDManager.VertexIDType.PartitionedVertex.is(vertex.longId()))) {
                //going through transaction so we can query deleted vertices
                final VertexCentricQueryBuilder qb = ((InternalVertex)vertex).tx().query(vertex);
                qb.noPartitionRestriction().type(key);
                if (onlyLoaded) qb.queryOnlyLoaded();
                props = qb.properties();
            } else {
                props = vertex.query().keys(key.name()).properties();
            }
            for (final JanusGraphVertexProperty p : props) {
                assert !onlyLoaded || p.isLoaded() || p.isRemoved();
                assert key.dataType().equals(p.value().getClass()) : key + " -> " + p;
                values.add(new RecordEntry(p));
            }
        }
        for (final RecordEntry value : values) {
            current[pos]=value;
            indexMatches(vertex,current,matches,fields,pos+1,onlyLoaded,replaceKey,replaceValue);
        }
    }

    private StaticBuffer getIndexKey(CompositeIndexType index, Object[] values) {
        final DataOutput out = serializer.getDataOutput(8*DEFAULT_OBJECT_BYTELEN + 8);
        VariableLong.writePositive(out, index.getID());
        final IndexField[] fields = index.getFieldKeys();
        Preconditions.checkArgument(fields.length>0 && fields.length==values.length);
        for (int i = 0; i < fields.length; i++) {
            final IndexField f = fields[i];
            final Object value = values[i];
            Preconditions.checkNotNull(value);
            if (InternalAttributeUtil.hasGenericDataType(f.getFieldKey())) {
                out.writeClassAndObject(value);
            } else {
                assert value.getClass().equals(f.getFieldKey().dataType()) : value.getClass() + " - " + f.getFieldKey().dataType();
                out.writeObjectNotNull(value);
            }
        }
        StaticBuffer key = out.getStaticBuffer();
        if (hashKeys) key = HashingUtil.hashPrefixKey(hashLength,key);
        return key;
    }

    private Entry getIndexEntry(CompositeIndexType index, RecordEntry[] record, JanusGraphElement element) {
        final DataOutput out = serializer.getDataOutput(1+8+8*record.length+4*8);
        out.putByte(FIRST_INDEX_COLUMN_BYTE);
        if (index.getCardinality()!= Cardinality.SINGLE) {
            VariableLong.writePositive(out,element.longId());
            if (index.getCardinality()!=Cardinality.SET) {
                for (final RecordEntry re : record) {
                    VariableLong.writePositive(out,re.relationId);
                }
            }
        }
        final int valuePosition=out.getPosition();
        if (element instanceof JanusGraphVertex) {
            VariableLong.writePositive(out,element.longId());
        } else {
            assert element instanceof JanusGraphRelation;
            final RelationIdentifier rid = (RelationIdentifier)element.id();
            final long[] longs = rid.getLongRepresentation();
            Preconditions.checkArgument(longs.length == 3 || longs.length == 4);
            for (final long aLong : longs) VariableLong.writePositive(out, aLong);
        }
        return new StaticArrayEntry(out.getStaticBuffer(),valuePosition);
    }

    private static class RecordEntry {

        final long relationId;
        final Object value;
        final PropertyKey key;

        private RecordEntry(long relationId, Object value, PropertyKey key) {
            this.relationId = relationId;
            this.value = value;
            this.key = key;
        }

        private RecordEntry(JanusGraphVertexProperty property) {
            this(property.longId(),property.value(),property.propertyKey());
        }
    }

    public static class IndexRecords extends ArrayList<RecordEntry[]> {

        @Override
        public boolean add(RecordEntry[] record) {
            return super.add(Arrays.copyOf(record,record.length));
        }

        public Iterable<Object[]> getRecordValues() {
            return Iterables.transform(this, new Function<RecordEntry[], Object[]>() {
                @Nullable
                @Override
                public Object[] apply(final RecordEntry[] record) {
                    return getValues(record);
                }
            });
        }

        private static Object[] getValues(RecordEntry[] record) {
            final Object[] values = new Object[record.length];
            for (int i = 0; i < values.length; i++) {
                values[i]=record[i].value;
            }
            return values;
        }
    }
}