/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.query;

/**
 * Specify the SQLite FTS
 * <a target="_blank" href="https://www.sqlite.org/fts3.html#tokenizer">
 * tokenizer</a> to use for indexing.
 */

public class Tokenizer {

    /**
     * The default tokenizer which is equivalent to
     * {@code new Tokenizer("simple")}
     */
    public static final Tokenizer DEFAULT = new Tokenizer("simple");

    // string representation of the tokenizer name eg "porter"
    public final String tokenizerName;

    // tokenizer arguments, may be null
    // only some tokenizers take arguments, for example with tokenizerName="icu" a valid argument
    // could be "en_AU"
    public final String tokenizerArguments;

    /**
     * Returns a custom tokenizer which does not take arguments
     * @param tokenizerName the name of the tokenizer, for example "porter"
     */
    public Tokenizer(String tokenizerName) {
        this(tokenizerName, null);
    }

    /**
     * Returns a custom tokenizer which takes one or more whitespace separated arguments
     * @param tokenizerName the name of the tokenizer, for example "icu"
     * @param tokenizerArguments a list of whitespace separated qualifiers to pass to the tokenizer
     *                           implementation, for example "en_AU"
     */
    public Tokenizer(String tokenizerName, String tokenizerArguments) {
        this.tokenizerName = tokenizerName;
        this.tokenizerArguments = tokenizerArguments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tokenizer tokenizer = (Tokenizer) o;

        if (!tokenizerName.equals(tokenizer.tokenizerName)) {
            return false;
        }
        return tokenizerArguments != null ? tokenizerArguments.equals(tokenizer
                .tokenizerArguments) : tokenizer.tokenizerArguments == null;

    }

    @Override
    public int hashCode() {
        int result = tokenizerName.hashCode();
        result = 31 * result + (tokenizerArguments != null ? tokenizerArguments.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tokenizer{" +
                "tokenizerName='" + tokenizerName + '\'' +
                ", tokenizerArguments='" + tokenizerArguments + '\'' +
                '}';
    }

}
