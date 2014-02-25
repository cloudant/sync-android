package com.cloudant.sync.datastore;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.TreeMap;

/**
 * Created by tomblench on 02/03/2014.
 */
public class OrderedMap<K, V> extends TreeMap<K, V> {

    private ArrayList<K> orderedKeys;

    public OrderedMap() {
        super();
        orderedKeys = new ArrayList<K>();
    }

    public ArrayList<K> getOrderedKeys() {
        return orderedKeys;
    }

    @Override
    public V put(K key, V value) {
        V val = super.put(key, value);
        orderedKeys.add(key);
        return val;
    }
}
