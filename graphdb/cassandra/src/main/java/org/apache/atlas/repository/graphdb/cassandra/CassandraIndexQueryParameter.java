package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasIndexQueryParameter;

public class CassandraIndexQueryParameter implements AtlasIndexQueryParameter {

    private final String parameterName;
    private final String parameterValue;

    public CassandraIndexQueryParameter(String parameterName, String parameterValue) {
        this.parameterName  = parameterName;
        this.parameterValue = parameterValue;
    }

    @Override
    public String getParameterName() {
        return parameterName;
    }

    @Override
    public String getParameterValue() {
        return parameterValue;
    }
}
