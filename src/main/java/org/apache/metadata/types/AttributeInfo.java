package org.apache.metadata.types;

import org.apache.metadata.MetadataException;

public class AttributeInfo {
    public final String name;
    private IDataType dataType;
    public final Multiplicity multiplicity;
    public final boolean isComposite;
    /**
     * If this is a reference attribute, then the name of the attribute on the Class
     * that this refers to.
     */
    public final String reverseAttributeName;

    AttributeInfo(TypeSystem t, AttributeDefinition def) throws MetadataException {
        this.name = def.name;
        this.dataType = t.getDataType(def.dataTypeName);
        this.multiplicity = def.multiplicity;
        this.isComposite = def.isComposite;
        this.reverseAttributeName = def.reverseAttributeName;
    }

    public IDataType dataType() {
        return dataType;
    }

    void setDataType(IDataType dT) {
        dataType = dT;
    }
}
