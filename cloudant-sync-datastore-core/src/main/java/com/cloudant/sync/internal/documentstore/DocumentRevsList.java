/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.documentstore;

import com.cloudant.sync.internal.mazha.DocumentRevs;
import com.cloudant.sync.internal.util.Misc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * <p>
 * List of <code>DocumentRevs</code>. All the <code>DocumentRevs</code> are for the same document.
 * If the DocumentRevisionTree is a forest, some of the <code>DocumentRevs</code> might from different tree (case 2).
 * If two <code>DocumentRevs</code> are from the same tree, they are different branch of that tree (case 1).
 * </p>
 *
 * <pre>
 * Case 1: they are from same tree.
 *   1 → 2  → 3
 *     |
 *     → 2* → 3*
 *
 * Case 2: they are from different trees
 *   1  → 2  → 3
 *
 *   1* → 2* → 3*
 * </pre>
 *
 * <p>
 * The list can be iterated in the order of minimum generation ID (min-generation). Each
 * <code>DocumentRevs</code> has a list of revisions ids (aka revision history), and "start".
 * The "start" number is largest generation. So the min-generation is:
 * </p>
 * <pre>
 *   DocumentRevs.getRevisions().getStart() → DocumentRevs.getRevisions().getIds().size() + 1.
 * </pre>
 * <p>
 * This is very important since it decides which <code>DocumentRevs</code> is inserted to db first.
 * </p>
 *
 * <p>
 * For <code>DocumentRevs</code> with the same "min-generation", the order is un-determined. This is
 * probably the case two document with same ID/body are created in different database.
 * </p>
 */
public class DocumentRevsList implements Iterable<DocumentRevs> {

    private final List<DocumentRevs> documentRevsList;

    public DocumentRevsList(List<DocumentRevs> list) {
        Misc.checkNotNull(list, "DocumentRevs list");
        this.documentRevsList = new ArrayList<DocumentRevs>(list);

        // Order of the list decides which DocumentRevs is inserted first in bulkCreateDocs update.
        Collections.sort(this.documentRevsList, new DocumentRevsComparator());
    }

    @Override
    public Iterator<DocumentRevs> iterator() {
        return this.documentRevsList.iterator();
    }

    public DocumentRevs get(int index) {
        return this.documentRevsList.get(index);
    }

    @Override
    public String toString() {
        return documentRevsList.toString();
    }

    private static class DocumentRevsComparator implements Comparator<DocumentRevs>, Serializable {

        private static final long serialVersionUID = 5278582092379780124L;

        @Override
        public int compare(DocumentRevs o1, DocumentRevs o2) {
            return getMinGeneration(o1) - getMinGeneration(o2);
        }

        /**
         * Get the minimum generation ID from the <code>DocumentRevs</code>
         * @see DocumentRevs
         */
        private int getMinGeneration(DocumentRevs o1) {
            return o1.getRevisions().getStart() - o1.getRevisions().getIds().size() + 1;
        }
    }
}
