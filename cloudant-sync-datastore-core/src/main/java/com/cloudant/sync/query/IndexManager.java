package com.cloudant.sync.query;

import java.util.List;
import java.util.Map;

/**
 * Created by tomblench on 03/10/2016.
 */

public interface IndexManager {

    Map<String, Object> listIndexes();

    String ensureIndexed(List<Object> fieldNames);

    String ensureIndexed(List<Object> fieldNames, String indexName);

    String ensureIndexed(List<Object> fieldNames, String indexName, IndexType indexType);

    String ensureIndexed(List<Object> fieldNames,
                         String indexName,
                         IndexType indexType,
                         Map<String, String> indexSettings);

    boolean deleteIndexNamed(final String indexName);

    boolean updateAllIndexes();

    boolean isTextSearchEnabled();

    // TODO we may not want to expose this publicly
    void close();

}
