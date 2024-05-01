package org.apache.atlas.service.redis;

import org.slf4j.Logger;

public interface RedisService {

  boolean acquireDistributedLock(String key) throws Exception;

  void releaseDistributedLock(String key);

  Logger getLogger();

}
