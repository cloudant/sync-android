package com.cloudant.common;

import com.google.common.collect.ForwardingMap;

import java.util.Map;

/**
 * Created by tomblench on 10/02/16.
 */
public class SimpleChangeNotifyingMap<K, V> extends ForwardingMap<K, V> {

    Map<K, V> delegateMap;
    private boolean mapChanged;

    public SimpleChangeNotifyingMap(Map<K, V> delegateMap) {
        this.delegateMap = delegateMap;
        this.mapChanged = false;
    }

    protected Map<K, V> delegate() {
        return delegateMap;
    }

    public boolean hasChanged() {
        return mapChanged;
    }


    @Override
    public V put(K key, V value) {
        mapChanged = true;
        return super.put(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        mapChanged = true;
        super.putAll(map);
    }

    @Override
    public V remove(Object object) {
        mapChanged = true;
        return super.remove(object);
    }

}
