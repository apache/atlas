package org.apache.metadata.types;

import com.google.common.collect.ImmutableList;
import org.apache.metadata.MetadataException;

import java.util.HashMap;
import java.util.Map;

public class TypeSystem {

    private Map<String, IDataType> types;

    public TypeSystem() throws MetadataException {
        types = new HashMap<String, IDataType>();
        registerPrimitiveTypes();
    }

    public ImmutableList<String> getTypeNames() {
        return ImmutableList.copyOf(types.keySet());
    }

    private void registerPrimitiveTypes() {
        types.put(DataTypes.BOOLEAN_TYPE.getName(), DataTypes.BOOLEAN_TYPE);
        types.put(DataTypes.BYTE_TYPE.getName(), DataTypes.BYTE_TYPE);
        types.put(DataTypes.SHORT_TYPE.getName(), DataTypes.SHORT_TYPE);
        types.put(DataTypes.INT_TYPE.getName(), DataTypes.INT_TYPE);
        types.put(DataTypes.LONG_TYPE.getName(), DataTypes.LONG_TYPE);
        types.put(DataTypes.FLOAT_TYPE.getName(), DataTypes.FLOAT_TYPE);
        types.put(DataTypes.DOUBLE_TYPE.getName(), DataTypes.DOUBLE_TYPE);
        types.put(DataTypes.BIGINTEGER_TYPE.getName(), DataTypes.BIGINTEGER_TYPE);
        types.put(DataTypes.BIGDECIMAL_TYPE.getName(), DataTypes.BIGDECIMAL_TYPE);
        types.put(DataTypes.DATE_TYPE.getName(), DataTypes.DATE_TYPE);
        types.put(DataTypes.STRING_TYPE.getName(), DataTypes.STRING_TYPE);
    }

    public IDataType getDataType(String name) throws MetadataException {
        if ( types.containsKey(name) ) {
            return types.get(name);
        }
        throw new MetadataException(String.format("Unknown datatype: %s", name));
    }

    public StructType defineStructType(String name,
                                       boolean errorIfExists,
                                       AttributeDefinition... attrDefs) throws MetadataException {
         if ( types.containsKey(name) ) {
            throw new MetadataException(String.format("Cannot redefine type %s", name));
        }
        assert name != null;
        AttributeInfo[] infos = new AttributeInfo[attrDefs.length];
        Map<Integer, AttributeDefinition> recursiveRefs = new HashMap<Integer, AttributeDefinition>();
        try {
            types.put(name, new StructType(name));
            for (int i = 0; i < attrDefs.length; i++) {
                infos[i] = new AttributeInfo(this, attrDefs[i]);
                if ( attrDefs[i].dataTypeName == name ) {
                    recursiveRefs.put(i, attrDefs[i]);
                }
            }
        } catch(MetadataException me) {
            types.remove(name);
            throw me;
        } catch(RuntimeException re) {
            types.remove(name);
            throw re;
        }
        StructType sT = new StructType(name, infos);
        types.put(name, sT);
        for(Map.Entry<Integer, AttributeDefinition> e : recursiveRefs.entrySet()) {
            infos[e.getKey()].setDataType(sT);
        }
        return sT;
    }

    public DataTypes.ArrayType defineArrayType(IDataType elemType) throws MetadataException {
        assert elemType != null;
        DataTypes.ArrayType dT = new DataTypes.ArrayType(elemType);
        types.put(dT.getName(), dT);
        return dT;
    }

    public DataTypes.MapType defineMapType(IDataType keyType, IDataType valueType) throws MetadataException {
        assert keyType != null;
        assert valueType != null;
        DataTypes.MapType dT =  new DataTypes.MapType(keyType, valueType);
        types.put(dT.getName(), dT);
        return dT;
    }
}
