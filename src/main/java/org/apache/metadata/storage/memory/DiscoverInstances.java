package org.apache.metadata.storage.memory;


import org.apache.metadata.IReferenceableInstance;
import org.apache.metadata.MetadataException;
import org.apache.metadata.storage.Id;
import org.apache.metadata.storage.RepositoryException;
import org.apache.metadata.types.DataTypes;
import org.apache.metadata.types.ObjectGraphWalker;

import java.util.HashMap;
import java.util.Map;

public class DiscoverInstances implements ObjectGraphWalker.NodeProcessor {

    final MemRepository memRepository;
    final Map<Id, Id> idToNewIdMap;
    final Map<Id, IReferenceableInstance> idToInstanceMap;

    public DiscoverInstances(MemRepository memRepository) {
        this.memRepository = memRepository;
        idToNewIdMap = new HashMap<Id, Id>();
        idToInstanceMap = new HashMap<Id, IReferenceableInstance>();
    }

    @Override
    public void processNode(ObjectGraphWalker.Node nd) throws MetadataException {

        IReferenceableInstance ref =  null;
        Id id = null;

        if ( nd.attributeName == null ) {
            ref = (IReferenceableInstance) nd.instance;
            id = ref.getId();
        } else if ( nd.dataType.getTypeCategory() == DataTypes.TypeCategory.CLASS ) {
            if ( nd.value != null && (nd.value instanceof  Id)) {
                id = (Id) nd.value;
            }
        }

        if ( id != null ) {
            if ( id.isUnassigned() ) {
                if ( !idToNewIdMap.containsKey(id)) {
                    idToNewIdMap.put(id, memRepository.newId(id.className));
                }
                if ( idToInstanceMap.containsKey(ref)) {
                    // Oops
                    throw new RepositoryException(
                            String.format("Unexpected internal error: Id %s processed again", id));
                }
                idToInstanceMap.put(id, ref);
            }
        }
    }
}
