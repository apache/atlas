package org.apache.metadata.storage.memory;

import org.apache.metadata.storage.IRepository;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class MemRepository implements IRepository {

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public DateFormat getDateFormat() {
        return dateFormat;
    }

    @Override
    public DateFormat getTimestampFormat() {
        return timestampFormat;
    }

    @Override
    public boolean allowNullsInCollections() {
        return false;
    }
}
