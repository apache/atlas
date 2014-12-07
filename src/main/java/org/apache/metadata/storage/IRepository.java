package org.apache.metadata.storage;

import java.text.DateFormat;

public interface IRepository {

    DateFormat getDateFormat();
    DateFormat getTimestampFormat();
    boolean allowNullsInCollections();
}
