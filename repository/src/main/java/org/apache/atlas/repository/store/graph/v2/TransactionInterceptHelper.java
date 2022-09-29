package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.annotation.GraphTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class TransactionInterceptHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionInterceptHelper.class);

    public TransactionInterceptHelper(){}

    @GraphTransaction
    public void intercept(){}
}
