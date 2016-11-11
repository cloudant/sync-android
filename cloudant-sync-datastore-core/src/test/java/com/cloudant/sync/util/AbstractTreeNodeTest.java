/**
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

package com.cloudant.sync.util;

import com.cloudant.sync.internal.util.AbstractTreeNode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

public class AbstractTreeNodeTest {

    TestTreeNode n1, n2, n3, n4;

    @Before
    public void startUp() {
        n1 = new TestTreeNode(Integer.valueOf(1));
        n2 = new TestTreeNode(Integer.valueOf(2));
        n3 = new TestTreeNode(Integer.valueOf(3));
        n4 = new TestTreeNode(Integer.valueOf(4));
    }

    @Test
    public void constructor() {
        Assert.assertEquals(Integer.valueOf(1), n1.getData());
        Assert.assertEquals(Integer.valueOf(2), n2.getData());
    }

    @Test
    public void constructor_allowNull() {
        TestTreeNode nn = new TestTreeNode(null);
        Assert.assertNull(nn.getData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void addChild_addNodeItSelf_notAllowed() {
        n1.addChild(n1);
    }

    @Test
    public void hasChildren() {
        Assert.assertFalse(n1.hasChildren());
        n1.addChild(n2);
        Assert.assertTrue(n1.hasChildren());
    }

    @Test
    public void numberOfChildren() {
        Assert.assertEquals(0, n1.numberOfChildren());

        n1.addChild(n2);
        Assert.assertEquals(1, n1.numberOfChildren());

        n1.addChild(n2);
        Assert.assertEquals(1, n1.numberOfChildren());

        n1.addChild(n3);
        Assert.assertEquals(2, n1.numberOfChildren());
    }

    @Test
    public void iterateChildren() {
        n1.addChild(n4);
        n1.addChild(n2);
        n1.addChild(n3);

        Iterator<AbstractTreeNode<Integer>> t = n1.iterateChildren();

        Assert.assertTrue(t.hasNext());
        TestTreeNode n = (TestTreeNode) t.next();
        Assert.assertEquals(Integer.valueOf(2), n.getData());

        Assert.assertTrue(t.hasNext());
        n = (TestTreeNode) t.next();
        Assert.assertEquals(Integer.valueOf(3), n.getData());

        Assert.assertTrue(t.hasNext());
        n = (TestTreeNode) t.next();
        Assert.assertEquals(Integer.valueOf(4), n.getData());
    }

    @Test
    public void depth_addInOrderOfDepth() {
        n1.addChild(n2);
        n2.addChild(n3);
        n3.addChild(n4);

        Assert.assertEquals(0, n1.depth());
        Assert.assertEquals(1, n2.depth());
        Assert.assertEquals(2, n3.depth());
        Assert.assertEquals(3, n4.depth());
    }

    @Test
    public void depth_addNotInOrderOfDepth() {
        n2.addChild(n3);
        n3.addChild(n4);
        Assert.assertEquals(0, n2.depth());
        Assert.assertEquals(1, n3.depth());
        Assert.assertEquals(2, n4.depth());

        n1.addChild(n2);

        Assert.assertEquals(0, n1.depth());
        Assert.assertEquals(1, n2.depth());
        Assert.assertEquals(2, n3.depth());
        Assert.assertEquals(3, n4.depth());
    }

    @Test
    public void comparable() {
        Assert.assertTrue(n1.compareTo(n2) < 0);
        Assert.assertTrue(n2.compareTo(n1) > 0);
        Assert.assertTrue(n1.compareTo(n1) == 0);
    }


    @Test
    public void equals() {
        TestTreeNode m = new TestTreeNode(Integer.valueOf(1));
        Assert.assertEquals(m, n1);
        Assert.assertNotSame(m, n2);
    }

    public static class TestTreeNode extends AbstractTreeNode<Integer> implements Comparable<AbstractTreeNode<Integer>> {
        public TestTreeNode(Integer data) {
            super(data);
        }

        @Override
        public int compareTo(AbstractTreeNode<Integer> o) {
            return getData().compareTo(o.getData());
        }
    }
}
