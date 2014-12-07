package org.apache.metadata;

import org.apache.metadata.storage.IRepository;
import org.apache.metadata.types.TypeSystem;

public class MetadataService {

    final IRepository repo;
    final TypeSystem typeSystem;

    public static final ThreadLocal<MetadataService> currentSvc = new ThreadLocal<MetadataService>();

    public static void setCurrentService(MetadataService svc) {
        currentSvc.set(svc);
    }

    public static MetadataService getCurrentService() throws MetadataException {
        MetadataService m = currentSvc.get();
        if ( m == null ) {
            throw new MetadataException("No MetadataService associated with current thread");
        }
        return m;
    }

    public static IRepository getCurrentRepository() throws MetadataException {
        MetadataService m = currentSvc.get();
        IRepository r = m == null ? null : m.getRepository();
        if ( r == null ) {
            throw new MetadataException("No Repository associated with current thread");
        }
        return r;
    }

    public static TypeSystem getCurrentTypeSystem() throws MetadataException {
        MetadataService m = currentSvc.get();
        TypeSystem t = m == null ? null : m.getTypeSystem();
        if ( t == null ) {
            throw new MetadataException("No TypeSystem associated with current thread");
        }
        return t;
    }

    public MetadataService(IRepository repo, TypeSystem typeSystem) {
        this.typeSystem = typeSystem;
        this.repo = repo;
    }

    public IRepository getRepository() {
        return repo;
    }

    public TypeSystem getTypeSystem() {
        return typeSystem;
    }
}
