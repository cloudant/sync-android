/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.datastore;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class DocumentRevisionTreeTest {

    DocumentBody b;
    BasicDocumentRevision d1, d2, d3, d4, d5;
    BasicDocumentRevision c3, c4;
    BasicDocumentRevision e1, e2, e3;
    BasicDocumentRevision f3, f4;

    BasicDocumentRevision x2, x3;
    BasicDocumentRevision y3;

    @Before
    public void startUp() {
        b = new BasicDocumentBody("{\"a\": \"haha\"}".getBytes());

        /**
         * d1 -> d2 -> d3 -> d4 -> d5
         *        |
         *        -> c3 -> c4
         *
         * e1 -> e2 -> e3
         *        |
         *         -> f3 -> f4
         **/

        BasicDocumentRevision.BasicDocumentRevisionOptions opts = new BasicDocumentRevision.BasicDocumentRevisionOptions();
        opts.docInternalId = 1l;

        opts.sequence = 1l;
        opts.parent = -1l;
        d1 = new BasicDocumentRevision( "id1", "1-rev", b, opts);

        opts.sequence = 2l;
        opts.parent = 1l;
        d2 = new BasicDocumentRevision( "id1", "2-rev", b, opts);

        opts.sequence = 3l;
        opts.parent = 2l;
        d3 = new BasicDocumentRevision( "id1", "3-rev", b, opts);

        opts.sequence = 4l;
        opts.parent = 3l;
        d4 = new BasicDocumentRevision( "id1", "4-rev", b, opts);

        opts.sequence = 5l;
        opts.parent = 4l;
        opts.current = true;
        d5 = new BasicDocumentRevision( "id1", "5-rev", b, opts);

        opts = new BasicDocumentRevision.BasicDocumentRevisionOptions();
        opts.sequence = 6l;
        opts.parent = 2l;

        c3 = new BasicDocumentRevision( "id1", "3-rev2", b, opts);
        opts.sequence = 7l;
        opts.parent = 6l;
        opts.current = true;
        c4 = new BasicDocumentRevision( "id1", "4-rev2", b, opts);

        opts = new BasicDocumentRevision.BasicDocumentRevisionOptions();
        opts.sequence = 8l;
        opts.parent = -1l;
        e1 = new BasicDocumentRevision( "id1", "1-rev-star", b, opts);

        opts.sequence = 9l;
        opts.parent = 8l;
        e2 = new BasicDocumentRevision( "id1", "2-rev-star", b, opts);

        opts.sequence = 10l;
        opts.parent = 9l;
        e3 = new BasicDocumentRevision( "id1", "3-rev-star", b, opts);

        opts.sequence = 11l;
        opts.parent = 9l;
        f3 = new BasicDocumentRevision( "id1", "3-rev-star-star", b, opts);

        opts.sequence = 12l;
        opts.parent = 11l;
        f4 = new BasicDocumentRevision( "id1", "4-rev-star-star", b, opts);

        /**
         * x2 -> x3
         *  |
         *    -> y3
         */

        opts = new BasicDocumentRevision.BasicDocumentRevisionOptions();
        opts.docInternalId = 2l;

        opts.sequence = 12l;
        opts.parent = -1l;
        x2 = new BasicDocumentRevision( "id2", "2-x", b, opts);

        opts.sequence = 13l;
        opts.parent = 12l;
        x3 = new BasicDocumentRevision( "id2", "3-x", b, opts);

        opts.sequence = 14l;
        opts.parent = 12l;
        y3 = new BasicDocumentRevision( "id2", "3-y", b, opts);
    }

    @Test
    public void constructor_nullRoot() {
        DocumentRevisionTree t = new DocumentRevisionTree();
        Assert.assertEquals(0, t.roots().size());
        t.add(d1);

        checkTreeWithOnlyRootNode(t);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_invalidRootNode() {
        DocumentRevisionTree t = new DocumentRevisionTree(d2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_invalidRootNode2() {
        DocumentRevisionTree t = new DocumentRevisionTree();
        t.add(d2);
    }

    @Test
    public void constructor() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);

        checkTreeWithOnlyRootNode(t);
    }

    private void checkTreeWithOnlyRootNode(DocumentRevisionTree t) {
        Assert.assertTrue(d1 == t.root(d1.getSequence()).getData());
        List<DocumentRevisionTree.DocumentRevisionNode> l = t.leafs();
        Assert.assertEquals(1, l.size());
        Assert.assertTrue(d1 == l.get(0).getData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void add_wrongOrder_exception () {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        t.add(d3);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void add_sameNodeAddedTwice_exception () {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        t.add(d1);
    }

    @Test
    public void add_oneTreeInOrderOfSequence() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);

        addOneTreeInOrderOfSequence(t);
    }

    private void addOneTreeInOrderOfSequence(DocumentRevisionTree t) {
        t.add(d2).add(d3).add(d4).add(d5);
        Assert.assertTrue(d1 == t.root(d1.getSequence()).getData());
        Assert.assertFalse(t.hasConflicts());
        Assert.assertEquals(1, t.leafs().size());
        Assert.assertTrue(t.leafs().contains(new DocumentRevisionTree.DocumentRevisionNode(d5)));

        t.add(c3).add(c4);
        Assert.assertTrue(d1 == t.root(d1.getSequence()).getData());
        Assert.assertTrue(t.hasConflicts());
        Assert.assertEquals(2, t.leafs().size());
        Assert.assertTrue(t.leafs().contains(new DocumentRevisionTree.DocumentRevisionNode(d5)));
        Assert.assertTrue(t.leafs().contains(new DocumentRevisionTree.DocumentRevisionNode(c4)));
    }

    @Test
    public void lookupBySequence_oneTree() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        t.add(d2).add(d3).add(d4).add(d5);
        t.add(c3).add(c4);

        Assert.assertNull(t.bySequence(-2l));
        BasicDocumentRevision d = t.bySequence(d2.getSequence());
        Assert.assertTrue(d.getSequence() == d2.getSequence());
    }

    @Test
    public void lookup_oneTree() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        t.add(d2).add(d3).add(d4).add(d5);
        t.add(c3).add(c4);

        BasicDocumentRevision d = t.lookup(d3.getId(), d3.getRevision());
        Assert.assertNotNull(d);

        BasicDocumentRevision m = t.lookup("haha", "hehe");
        Assert.assertNull(m);
    }

    @Test
    public void depth_oneTree() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        t.add(d2).add(d3).add(d4).add(d5);
        t.add(c3).add(c4);

        Assert.assertEquals(0, t.depth(d1.getSequence()));
        Assert.assertEquals(4, t.depth(d5.getSequence()));
        Assert.assertEquals(3, t.depth(c4.getSequence()));
        Assert.assertEquals(-1, t.depth(100l));
    }

    @Test
    public void leafs_oneTree() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        t.add(d2).add(d3).add(d4).add(d5);
        t.add(c3).add(c4);

        Assert.assertEquals(2, t.leafs().size());
        List<BasicDocumentRevision> l = new ArrayList<BasicDocumentRevision>();
        for(DocumentRevisionTree.DocumentRevisionNode n : t.leafs()) {
            l.add(n.getData());
        }
        Assert.assertTrue(l.contains(d5));
        Assert.assertTrue(l.contains(c4));
    }

    @Test
    public void leafRevisions_emptyTree() {
        DocumentRevisionTree t = new DocumentRevisionTree();
        Assert.assertThat(t.leafRevisionIds(), hasSize(0));
    }

    @Test
    public void leafRevisions_oneTree() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        t.add(d2).add(d3).add(d4).add(d5);
        t.add(c3).add(c4);

        Assert.assertThat(t.leafRevisionIds(), hasSize(2));
        Assert.assertThat(t.leafRevisionIds(), hasItems(d5.getRevision(), c4.getRevision()));
    }

    @Test
    public void getPathForLeaf_oneTreeWithLeafNodes_correctPathShouldBeReturned() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        t.add(d2).add(d3).add(d4).add(d5);
        t.add(c3).add(c4);

        {
            List<BasicDocumentRevision> p = t.getPathForNode(d5.getSequence());
            Assert.assertEquals(5, p.size());
            Assert.assertEquals(p.get(4).getSequence(), d1.getSequence());
            Assert.assertEquals(p.get(3).getSequence(), d2.getSequence());
            Assert.assertEquals(p.get(2).getSequence(), d3.getSequence());
            Assert.assertEquals(p.get(1).getSequence(), d4.getSequence());
            Assert.assertEquals(p.get(0).getSequence(), d5.getSequence());
        }


        {
            List<BasicDocumentRevision> p2 = t.getPathForNode(c4.getSequence());
            Assert.assertEquals(4, p2.size());
            Assert.assertEquals(p2.get(3).getSequence(), d1.getSequence());
            Assert.assertEquals(p2.get(2).getSequence(), d2.getSequence());
            Assert.assertEquals(p2.get(1).getSequence(), c3.getSequence());
            Assert.assertEquals(p2.get(0).getSequence(), c4.getSequence());
        }
    }

    @Test
    public void getPath_oneTreeWithLeafNodes_correctPathShouldBeReturned() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        t.add(d2).add(d3).add(d4).add(d5);
        t.add(c3).add(c4);

        {
            List<String> p = t.getPath(d5.getSequence());
            Assert.assertEquals(5, p.size());
            Assert.assertThat(p, equalTo(Arrays.asList(d5.getRevision(), d4.getRevision(), d3.getRevision(),
                    d2.getRevision(), d1.getRevision())));
        }


        {
            List<String> p2 = t.getPath(c4.getSequence());
            Assert.assertEquals(4, p2.size());
            Assert.assertThat(p2, equalTo(Arrays.asList(c4.getRevision(), c3.getRevision(), d2.getRevision(),
                    d1.getRevision())));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void getPath_invalidSequence_exception() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        t.add(d2).add(d3).add(d4).add(d5);
        t.add(c3).add(c4);
        t.getPath(1001L);
    }

    @Test
    public void getPath_treeWithOneRevision_correctTreeIsReturned() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        List<String> p = t.getPath(d1.getSequence());
        Assert.assertThat(p, hasSize(1));
        Assert.assertThat(p, equalTo(Arrays.asList(d1.getRevision())));
    }

    @Test
    public void add_twoTreesAndNodesInOrderOfSequence_treesShouldBeConstructedCorrectly() {
        DocumentRevisionTree t = new DocumentRevisionTree(d1);
        addOneTreeInOrderOfSequence(t);

        t.add(e1).add(e2).add(e3);
        Assert.assertEquals(2, t.roots().size());
        Assert.assertThat(t.roots().keySet(), hasItems(d1.getSequence(), e1.getSequence()));
        Assert.assertEquals(3, t.leafs().size());
        Assert.assertThat(leafSequences(t.leafs()), hasItems(d5.getSequence(), c4.getSequence(), e3.getSequence()));

        t.add(f3).add(f4);
        Assert.assertEquals(2, t.roots().size());
        Assert.assertEquals(4, t.leafs().size());
        Assert.assertThat(leafSequences(t.leafs()), hasItems(d5.getSequence(), c4.getSequence(), e3.getSequence(),
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
