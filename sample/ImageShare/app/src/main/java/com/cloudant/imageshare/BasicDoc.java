package com.cloudant.imageshare;

import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.util.JSONUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by pettyurin on 7/1/14.
 */
public class BasicDoc implements DocumentBody {

    Map<String, Object> doc;

    public BasicDoc(String value1, String value2) {
        doc = new HashMap<String, Object>(5);
        doc.put(value1, value2);
    }

    public Map<String, Object> asMap(){
        return doc;
    };

    public byte[] asBytes() {
        return JSONUtils.serializeAsBytes(doc);
    };
}
