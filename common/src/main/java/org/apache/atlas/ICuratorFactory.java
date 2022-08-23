package org.apache.atlas;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;

public interface ICuratorFactory {
    InterProcessMutex lockInstance(String zkRoot, String lockName);
}
