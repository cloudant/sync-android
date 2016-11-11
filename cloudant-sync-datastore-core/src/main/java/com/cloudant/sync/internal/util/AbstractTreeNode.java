/*
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

package com.cloudant.sync.internal.util;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * Internal utility class
 * @param <T> The type of data owned by each node
 * @api_private
 */
public abstract class AbstractTreeNode<T> {

    private final static Logger logger = Logger.getLogger(AbstractTreeNode.class.getCanonicalName());

    private T data;
    private int depth = 0;
    private Set<AbstractTreeNode<T>> children;


    public AbstractTreeNode(T data) {
        children = new TreeSet<AbstractTreeNode<T>>();
        setData(data);
    }

    public Iterator<AbstractTreeNode<T>> iterateChildren() {
        return this.children.iterator();
    }

    public int numberOfChildren() {
        return this.children.size();
    }

    public boolean hasChildren() {
        return (this.children.size() > 0);
    }

    public void addChild(AbstractTreeNode<T> child) {
        assert child != null;
        if( child == this ) {
            throw new IllegalArgumentException("Can not add node to its own child.");
        }

        children.add(child);
        child.depth = this.depth + 1;
        calculateChildrenDepth(child);
    }

    private void calculateChildrenDepth(AbstractTreeNode<T> node) {
        if(!node.hasChildren()) {
            return;
        } else {
            for(AbstractTreeNode<T> c : node.children) {
                c.depth = node.depth + 1;
                calculateChildrenDepth(c);
            }
        }
    }

    @Override
    public int hashCode() {
        return getData().hashCode();
    }

    @Override
    public boolean equals(Object that) {
        logger.entering("AbstractTreeNode","equals",that);
        if(!(that instanceof AbstractTreeNode)) {
            return false;
        }

        AbstractTreeNode a = (AbstractTreeNode)that;
        return this.getData().equals(a.getData());
    }

    public T getData() {
        return this.data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String toString() {
        return getData().toString();
    }

    public boolean equals(AbstractTreeNode<T> node) {
        return node.getData().equals(getData());
    }

    public int depth() {
        return this.depth;
    }
}
