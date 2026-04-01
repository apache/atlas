package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;

public class CassandraPropertyKey implements AtlasPropertyKey {

    private final String          name;
    private final Class           propertyClass;
    private final AtlasCardinality cardinality;

    public CassandraPropertyKey(String name, Class propertyClass, AtlasCardinality cardinality) {
        this.name          = name;
        this.propertyClass = propertyClass;
        this.cardinality   = cardinality;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public AtlasCardinality getCardinality() {
        return cardinality;
    }

    public Class getPropertyClass() {
        return propertyClass;
    }
}
