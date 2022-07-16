package org.apache.atlas;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;

public interface ICuratorFactory {
    InterProcessMutex lockInstanceWithDefaultZkRoot(String lockName);
}
