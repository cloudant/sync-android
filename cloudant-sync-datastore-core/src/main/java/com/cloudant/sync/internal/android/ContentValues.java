/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Modifications allow class to work outside Android framework by
 * Cloudant, Inc., Copyright © 2013 Cloudant, Inc.
 *
 * Copyright © 2007 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudant.sync.internal.android;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ContentValues {

    public static final String TAG = "ContentValues";
    private static final Logger logger = Logger.getLogger(ContentValues.class.getCanonicalName());


    /** Holds the actual values */
    private HashMap<String, Object> mValues;

    /**
     * Creates an empty set of values using the default initial size
     */
    public ContentValues() {
        // Choosing a default size of 8 based on analysis of typical
        // consumption by applications.
        mValues = new HashMap<String, Object>(8);
    }

    /**
     * Creates an empty set of values using the given initial size
     *
     * @param size the initial size of the set of values
     */
    public ContentValues(int size) {
        mValues = new HashMap<String, Object>(size, 1.0f);
    }

    /**
     * Creates a set of values copied from the given set
     *
     * @param from the values to copy
     */
    public ContentValues(ContentValues from) {
        mValues = new HashMap<String, Object>(from.mValues);
    }

    /**
     * Creates a set of values copied from the given HashMap. This is used
     * by the Parcel unmarshalling code.
     *
     * @param values the values to start with
     * {@hide}
     */
    private ContentValues(HashMap<String, Object> values) {
        mValues = values;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof ContentValues)) {
            return false;
        }
        return mValues.equals(((ContentValues) object).mValues);
    }

    @Override
    public int hashCode() {
        return mValues.hashCode();
    }

    /**
     * Adds a value to the set.
     *
     * @param key the name of the value to forceInsert
     * @param value the data for the value to forceInsert
     */
    public ContentValues put(String key, String value) {
        mValues.put(key, value);
        return this;
    }

    /**
     * Adds all values from the passed in ContentValues.
     *
     * @param other the ContentValues from which to copy
     */
    public ContentValues putAll(ContentValues other) {
        mValues.putAll(other.mValues);
        return this;
    }

    /**
     * Adds a value to the set.
     *
     * @param key the name of the value to forceInsert
     * @param value the data for the value to forceInsert
     */
    public ContentValues put(String key, Byte value) {
        mValues.put(key, value);
        return this;
    }

    /**
     * Adds a value to the set.
     *
     * @param key the name of the value to forceInsert
     * @param value the data for the value to forceInsert
     */
    public ContentValues put(String key, Short value) {
        mValues.put(key, value);
        return this;
    }

    /**
     * Adds a value to the set.
     *
     * @param key the name of the value to forceInsert
     * @param value the data for the value to forceInsert
     */
    public ContentValues put(String key, Integer value) {
        mValues.put(key, value);
        return this;
    }

    /**
     * Adds a value to the set.
     *
     * @param key the name of the value to forceInsert
     * @param value the data for the value to forceInsert
     */
    public ContentValues put(String key, Long value) {
        mValues.put(key, value);
        return this;
    }

    /**
     * Adds a value to the set.
     *
     * @param key the name of the value to forceInsert
     * @param value the data for the value to forceInsert
     */
    public ContentValues put(String key, Float value) {
        mValues.put(key, value);
        return this;
    }

    /**
     * Adds a value to the set.
     *
     * @param key the name of the value to forceInsert
     * @param value the data for the value to forceInsert
     */
    public ContentValues put(String key, Double value) {
        mValues.put(key, value);
        return this;
    }

    /**
     * Adds a value to the set.
     *
     * @param key the name of the value to forceInsert
     * @param value the data for the value to forceInsert
     */
    public ContentValues put(String key, Boolean value) {
        mValues.put(key, value);
        return this;
    }

    /**
     * Adds a value to the set.
     *
     * @param key the name of the value to forceInsert
     * @param value the data for the value to forceInsert
     */
    public ContentValues put(String key, byte[] value) {
        mValues.put(key, value);
        return this;
    }

    /**
     * Adds a null value to the set.
     *
     * @param key the name of the value to make null
     */
    public ContentValues putNull(String key) {
        mValues.put(key, null);
        return this;
    }

    /**
     * Returns the number of values.
     *
     * @return the number of values
     */
    public int size() {
        return mValues.size();
    }

    /**
     * Remove a single value.
     *
     * @param key the name of the value to remove
     */
    public void remove(String key) {
        mValues.remove(key);
    }

    /**
     * Removes all values.
     */
    public void clear() {
        mValues.clear();
    }

    /**
     * Returns true if this object has the named value.
     *
     * @param key the value to check for
     * @return {@code true} if the value is present, {@code false} otherwise
     */
    public boolean containsKey(String key) {
        return mValues.containsKey(key);
    }

    /**
     * Gets a value. Valid value types are {@link String}, {@link Boolean}, and
     * {@link Number} implementations.
     *
     * @param key the value to get
     * @return the data for the value
     */
    public Object get(String key) {
        return mValues.get(key);
    }

    /**
     * Gets a value and converts it to a String.
     *
     * @param key the value to get
     * @return the String for the value
     */
    public String getAsString(String key) {
        Object value = mValues.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Gets a value and converts it to a Long.
     *
     * @param key the value to get
     * @return the Long value, or null if the value is missing or cannot be converted
     */
    public Long getAsLong(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).longValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Long.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    logger.log(Level.SEVERE,"Cannot parse Long value for " + value + " at key "+ key);
                    return null;
                }
            } else {
                logger.log(Level.SEVERE,"Cannot case value for "+ key + "to a Long "+ value,e);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to an Integer.
     *
     * @param key the value to get
     * @return the Integer value, or null if the value is missing or cannot be converted
     */
    public Integer getAsInteger(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).intValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Integer.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    logger.severe("Cannot parse Integer value for "+value+" at key"+ key);
                    return null;
                }
            } else {
                logger.log(Level.SEVERE,"Cannot cast value for "+ key + " to a Integer "+ value,e);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to a Short.
     *
     * @param key the value to get
     * @return the Short value, or null if the value is missing or cannot be converted
     */
    public Short getAsShort(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).shortValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Short.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    logger.severe("Cannot parse Short value for " + value + " at key " + key);
                    return null;
                }
            } else {
                logger.log(Level.SEVERE,"Cannot cast value for " + key + " to a Short: " + value, e);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to a Byte.
     *
     * @param key the value to get
     * @return the Byte value, or null if the value is missing or cannot be converted
     */
    public Byte getAsByte(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).byteValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Byte.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    logger.severe("Cannot parse Byte value for " + value + " at key " + key);
                    return null;
                }
            } else {
                logger.log(Level.SEVERE, "Cannot cast value for " + key + " to a Byte: " + value, e);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to a Double.
     *
     * @param key the value to get
     * @return the Double value, or null if the value is missing or cannot be converted
     */
    public Double getAsDouble(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).doubleValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Double.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    logger.severe("Cannot parse Double value for " + value + " at key " + key);
                    return null;
                }
            } else {
                logger.log(Level.SEVERE, "Cannot cast value for " + key + " to a Double: " + value, e);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to a Float.
     *
     * @param key the value to get
     * @return the Float value, or null if the value is missing or cannot be converted
     */
    public Float getAsFloat(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).floatValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Float.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    logger.severe( "Cannot parse Float value for " + value + " at key " + key);
                    return null;
                }
            } else {
                logger.log(Level.SEVERE, "Cannot cast value for " + key + " to a Float: " + value, e);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to a Boolean.
     *
     * @param key the value to get
     * @return the Boolean value, or false if the value is missing or cannot be converted
     */
    public Boolean getAsBoolean(String key) {
        Object value = mValues.get(key);
        try {
            return (Boolean) value;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                return Boolean.valueOf(value.toString());
            } else if (value instanceof Number) {
                return ((Number) value).intValue() != 0;
            } else {
                logger.log(Level.SEVERE, "Cannot cast value for " + key + " to a Boolean: " + value, e);
                return false;
            }
        }
    }

    /**
     * Gets a value that is a byte array. Note that this method will not convert
     * any other types to byte arrays.
     *
     * @param key the value to get
     * @return the byte[] value, or an empty byte array the value is missing or not a byte[]
     */
    public byte[] getAsByteArray(String key) {
        Object value = mValues.get(key);
        if (value instanceof byte[]) {
            return (byte[]) value;
        } else {
            return new byte[]{};
        }
    }

    /**
     * Returns a set of all of the keys and values
     *
     * @return a set of all of the keys and values
     */
    public Set<Map.Entry<String, Object>> valueSet() {
        return mValues.entrySet();
    }

    /**
     * Returns a set of all of the keys
     *
     * @return a set of all of the keys
     */
    public Set<String> keySet() {
        return mValues.keySet();
    }


    public int describeContents() {
        return 0;
    }

    /**
     * Returns a string containing a concise, human-readable description of this object.
     * @return a printable representation of this object.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String name : mValues.keySet()) {
            String value = getAsString(name);
            if (sb.length() > 0) sb.append(" ");
            sb.append(name + "=" + value);
        }
        return sb.toString();
    }
}
