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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

import com.cloudant.sync.documentstore.DocumentBody;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DocumentRevisionTreeTest {

    DocumentBody b;
    InternalDocumentRevision c1, c2, c3, c4, c5;
    InternalDocumentRevision d3, d4;
    InternalDocumentRevision e1, e2, e3;
    InternalDocumentRevision f3, f4;

    InternalDocumentRevision x2, x3;
    InternalDocumentRevision y3;

    @Before
    public void startUp() {
        b = new DocumentBodyImpl("{\"a\": \"haha\"}".getBytes());

        /**
         * c1 -> c2 -> c3 -> c4 -> c5
         *        |
         *        -> d3 -> d4
         *
         * e1 -> e2 -> e3
         *        |
         *         -> f3 -> f4
         **/

        DocumentRevisionBuilder.DocumentRevisionOptions opts = new DocumentRevisionBuilder.DocumentRevisionOptions();
        opts.docInternalId = 1l;

        opts.sequence = 1l;
        opts.parent = -1l;
        opts.current = false;
        c1 = new InternalDocumentRevision( "id1", "1-rev", b, opts);

        opts.sequence = 2l;
        opts.parent = 1l;
        opts.current = false;
        c2 = new InternalDocumentRevision( "id1", "2-rev", b, opts);

        opts.sequence = 3l;
        opts.parent = 2l;
        opts.current = false;
        c3 = new InternalDocumentRevision( "id1", "3-rev", b, opts);

        opts.sequence = 4l;
        opts.parent = 3l;
        opts.current = false;
        c4 = new InternalDocumentRevision( "id1", "4-rev", b, opts);

        opts.sequence = 5l;
        opts.parent = 4l;
        opts.current = true;
        c5 = new InternalDocumentRevision( "id1", "5-rev", b, opts);

        opts.sequence = 6l;
        opts.parent = 2l;
        opts.current = false;
        d3 = new InternalDocumentRevision( "id1", "3-rev2", b, opts);

        opts.sequence = 7l;
        opts.parent = 6l;
        opts.current = true;
        d4 = new InternalDocumentRevision( "id1", "4-rev2", b, opts);

        opts.sequence = 8l;
        opts.parent = -1l;
        opts.current = false;
        e1 = new InternalDocumentRevision( "id1", "1-rev-star", b, opts);

        opts.sequence = 9l;
        opts.parent = 8l;
        opts.current = false;
        e2 = new InternalDocumentRevision( "id1", "2-rev-star", b, opts);

        opts.sequence = 10l;
        opts.parent = 9l;
        opts.current = false;
        e3 = new InternalDocumentRevision( "id1", "3-rev-star", b, opts);

        opts.sequence = 11l;
        opts.parent = 9l;
        opts.current = false;
        f3 = new InternalDocumentRevision( "id1", "3-rev-star-star", b, opts);

        opts.sequence = 12l;
        opts.parent = 11l;
        opts.current = false;
        f4 = new InternalDocumentRevision( "id1", "4-rev-star-star", b, opts);

        /**
         * x2 -> x3
         *  |
         *    -> y3
         */

        opts = new DocumentRevisionBuilder.DocumentRevisionOptions();
        opts.docInternalId = 2l;
        opts.sequence = 12l;
        opts.parent = -1l;
        opts.current = false;
        x2 = new InternalDocumentRevision( "id2", "2-x", b, opts);

        opts.sequence = 13l;
        opts.parent = 12l;
        opts.current = true;
        x3 = new InternalDocumentRevision( "id2", "3-x", b, opts);

        opts.sequence = 14l;
        opts.parent = 12l;
        opts.current = false;
        y3 = new InternalDocumentRevision( "id2", "3-y", b, opts);
    }

    @Test
    public void constructor_nullRoot() {
        DocumentRevisionTree t = new DocumentRevisionTree();
        Assert.assertEquals(0, t.roots().size());
        t.add(c1);

        checkTreeWithOnlyRootNode(t);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_invalidRootNode() {
        DocumentRevisionTree t = new DocumentRevisionTree(c2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_invalidRootNode2() {
        DocumentRevisionTree t = new DocumentRevisionTree();
        t.add(c2);
    }

    @Test
    public void constructor() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);

        checkTreeWithOnlyRootNode(t);
    }

    private void checkTreeWithOnlyRootNode(DocumentRevisionTree t) {
        Assert.assertTrue(c1 == t.root(c1.getSequence()).getData());
        List<DocumentRevisionTree.DocumentRevisionNode> l = t.leafs();
        Assert.assertEquals(1, l.size());
        Assert.assertTrue(c1 == l.get(0).getData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void add_wrongOrder_exception () {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        t.add(c3);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void add_sameNodeAddedTwice_exception () {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        t.add(c1);
    }

    @Test
    public void add_oneTreeInOrderOfSequence() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);

        addOneTreeInOrderOfSequence(t);
    }

    private void addOneTreeInOrderOfSequence(DocumentRevisionTree t) {
        t.add(c2).add(c3).add(c4).add(c5);
        Assert.assertTrue(c1 == t.root(c1.getSequence()).getData());
        Assert.assertFalse(t.hasConflicts());
        Assert.assertEquals(1, t.leafs().size());
        Assert.assertTrue(t.leafs().contains(new DocumentRevisionTree.DocumentRevisionNode(c5)));

        t.add(d3).add(d4);
        Assert.assertTrue(c1 == t.root(c1.getSequence()).getData());
        Assert.assertTrue(t.hasConflicts());
        Assert.assertEquals(2, t.leafs().size());
        Assert.assertTrue(t.leafs().contains(new DocumentRevisionTree.DocumentRevisionNode(c5)));
        Assert.assertTrue(t.leafs().contains(new DocumentRevisionTree.DocumentRevisionNode(d4)));
    }

    @Test
    public void lookupBySequence_oneTree() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        t.add(c2).add(c3).add(c4).add(c5);
        t.add(d3).add(d4);

        Assert.assertNull(t.bySequence(-2l));
        InternalDocumentRevision d = t.bySequence(c2.getSequence());
        Assert.assertTrue(d.getSequence() == c2.getSequence());
    }

    @Test
    public void lookup_oneTree() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        t.add(c2).add(c3).add(c4).add(c5);
        t.add(d3).add(d4);

        InternalDocumentRevision d = t.lookup(c3.getId(), c3.getRevision());
        Assert.assertNotNull(d);

        InternalDocumentRevision m = t.lookup("haha", "hehe");
        Assert.assertNull(m);
    }

    @Test
    public void depth_oneTree() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        t.add(c2).add(c3).add(c4).add(c5);
        t.add(d3).add(d4);

        Assert.assertEquals(0, t.depth(c1.getSequence()));
        Assert.assertEquals(4, t.depth(c5.getSequence()));
        Assert.assertEquals(3, t.depth(d4.getSequence()));
        Assert.assertEquals(-1, t.depth(100l));
    }

    @Test
    public void leafs_oneTree() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        t.add(c2).add(c3).add(c4).add(c5);
        t.add(d3).add(d4);

        Assert.assertEquals(2, t.leafs().size());
        List<InternalDocumentRevision> l = new ArrayList<InternalDocumentRevision>();
        for(DocumentRevisionTree.DocumentRevisionNode n : t.leafs()) {
            l.add(n.getData());
        }
        Assert.assertTrue(l.contains(c5));
        Assert.assertTrue(l.contains(d4));
    }

    @Test
    public void leafRevisions_emptyTree() {
        DocumentRevisionTree t = new DocumentRevisionTree();
        Assert.assertThat(t.leafRevisionIds(), hasSize(0));
    }

    @Test
    public void leafRevisions_oneTree() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        t.add(c2).add(c3).add(c4).add(c5);
        t.add(d3).add(d4);

        Assert.assertThat(t.leafRevisionIds(), hasSize(2));
        Assert.assertThat(t.leafRevisionIds(), hasItems(c5.getRevision(), d4.getRevision()));
    }

    @Test
    public void getPathForLeaf_oneTreeWithLeafNodes_correctPathShouldBeReturned() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        t.add(c2).add(c3).add(c4).add(c5);
        t.add(d3).add(d4);

        {
            List<InternalDocumentRevision> p = t.getPathForNode(c5.getSequence());
            Assert.assertEquals(5, p.size());
            Assert.assertEquals(p.get(4).getSequence(), c1.getSequence());
            Assert.assertEquals(p.get(3).getSequence(), c2.getSequence());
            Assert.assertEquals(p.get(2).getSequence(), c3.getSequence());
            Assert.assertEquals(p.get(1).getSequence(), c4.getSequence());
            Assert.assertEquals(p.get(0).getSequence(), c5.getSequence());
        }


        {
            List<InternalDocumentRevision> p2 = t.getPathForNode(d4.getSequence());
            Assert.assertEquals(4, p2.size());
            Assert.assertEquals(p2.get(3).getSequence(), c1.getSequence());
            Assert.assertEquals(p2.get(2).getSequence(), c2.getSequence());
            Assert.assertEquals(p2.get(1).getSequence(), d3.getSequence());
            Assert.assertEquals(p2.get(0).getSequence(), d4.getSequence());
        }
    }

    @Test
    public void getPath_oneTreeWithLeafNodes_correctPathShouldBeReturned() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        t.add(c2).add(c3).add(c4).add(c5);
        t.add(d3).add(d4);

        {
            List<String> p = t.getPath(c5.getSequence());
            Assert.assertEquals(5, p.size());
            Assert.assertThat(p, equalTo(Arrays.asList(c5.getRevision(), c4.getRevision(),
                    c3.getRevision(),
                    c2.getRevision(), c1.getRevision())));
        }


        {
            List<String> p2 = t.getPath(d4.getSequence());
            Assert.assertEquals(4, p2.size());
            Assert.assertThat(p2, equalTo(Arrays.asList(d4.getRevision(), d3.getRevision(),
                    c2.getRevision(),
                    c1.getRevision())));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void getPath_invalidSequence_exception() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        t.add(c2).add(c3).add(c4).add(c5);
        t.add(d3).add(d4);
        t.getPath(1001L);
    }

    @Test
    public void getPath_treeWithOneRevision_correctTreeIsReturned() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        List<String> p = t.getPath(c1.getSequence());
        Assert.assertThat(p, hasSize(1));
        Assert.assertThat(p, equalTo(Arrays.asList(c1.getRevision())));
    }

    @Test
    public void add_twoTreesAndNodesInOrderOfSequence_treesShouldBeConstructedCorrectly() {
        DocumentRevisionTree t = new DocumentRevisionTree(c1);
        addOneTreeInOrderOfSequence(t);

        t.add(e1).add(e2).add(e3);
        Assert.assertEquals(2, t.roots().size());
        Assert.assertThat(t.roots().keySet(), hasItems(c1.getSequence(), e1.getSequence()));
        Assert.assertEquals(3, t.leafs().size());
        Assert.assertThat(leafSequences(t.leafs()), hasItems(c5.getSequence(), d4.getSequence(),
                e3.getSequence()));

        t.add(f3).add(f4);
        Assert.assertEquals(2, t.roots().size());
        Assert.assertEquals(4, t.leafs().size());
        Assert.assertThat(leafSequences(t.leafs()), hasItems(c5.getSequence(), d4.getSequence(),
                e3.getSequence(),
                f4.getSequence()));

    }

    List<Long> leafSequences(List<DocumentRevisionTree.DocumentRevisionNode> leafs) {
        List<Long> s = new ArrayList<Long>();
        for(DocumentRevisionTree.DocumentRevisionNode n : leafs) {
            s.add(n.getData().getSequence());
        }
        return s;
    }

    @Test
    public void add_rootRevisionStartFrom2_treeShouldBeConstructedCorrectly() {
        DocumentRevisionTree t = new DocumentRevisionTree(x2);
        t.add(x3).add(y3);

        Assert.assertEquals(2, t.leafs().size());
        Assert.assertEquals(1, t.roots().size());
    }
}
