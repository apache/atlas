package org.apache.metadata.types;


public final class AttributeDefinition {

    public final String name;
    public final String dataTypeName;
    public final Multiplicity multiplicity;
    public final boolean isComposite;
    /**
     * If this is a reference attribute, then the name of the attribute on the Class
     * that this refers to.
     */
    public final String reverseAttributeName;

    public AttributeDefinition(String name, String dataTypeName, Multiplicity multiplicity,
                               boolean isComposite, String reverseAttributeName) {
        this.name = name;
        this.dataTypeName = dataTypeName;
        this.multiplicity = multiplicity;
        this.isComposite = isComposite;
        this.reverseAttributeName = reverseAttributeName;
    }
}
