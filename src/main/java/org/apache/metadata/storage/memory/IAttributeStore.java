package org.apache.metadata.storage.memory;


import org.apache.metadata.ITypedInstance;
import org.apache.metadata.storage.RepositoryException;
import org.apache.metadata.storage.StructInstance;
import org.apache.metadata.types.IConstructableType;

public interface IAttributeStore {
    /**
     * Store the attribute's value from the 'instance' into this store.
     * @param pos
     * @param instance
     * @throws RepositoryException
     */
    void store(int pos, IConstructableType type, StructInstance instance) throws RepositoryException;

    /**
     * load the Instance with the value from position 'pos' for the attribute.
     * @param pos
     * @param instance
     * @throws RepositoryException
     */
    void load(int pos, IConstructableType type, StructInstance instance) throws RepositoryException;

    /**
     * Ensure store have space for the given pos.
     * @param pos
     * @throws RepositoryException
     */
    void ensureCapacity(int pos) throws RepositoryException;
}
