/**
 * Copyright (C) 2013 Cloudant
 *
 * Copyright (C) 2011 Ahmed Yehia (ahmed.yehia.m@gmail.com)
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

package com.cloudant.sync.internal.mazha;

import com.cloudant.sync.internal.common.CouchConstants;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a typical response about the a others along its revisions history information. This is used by a
 * replicator. It only need the content of the latest revision. All the previous revision only have the revision id.
 *
 * {
 * "_id": "cdb1a2fec33d146fe07a44ea823bf3ae"
 * "_rev": "4-47d7102726fc89914431cb217ab7bace",
 * "_revisions": {
 *   "start": 4
 *   "ids": [
 *   "47d7102726fc89914431cb217ab7bace",
 *   "d8e1fb8127d8dd732d9ae46a6c38ae3c",
 *   "74e0572530e3b4cd4776616d2f591a96",
 *   "421ff3d58df47ea6c5e83ca65efb2fa9"
 *   ],
 * },
 * "album": "A Flush Of Blood To My Head",
 * "title": "Trouble Two",
 * }
 *
 * TODO: need to think about how to better handle boolean serialization/de-serialization, since it could be absence,
 *       null, true and false for de-serialization, and when we do not need to put it into the JSON if it is false
 *       during serialization.
 *
 * @api_private
 */
public class DocumentRevs {

    @JsonProperty(CouchConstants._id)
    private String id;

    @JsonProperty(CouchConstants._rev)
    private String rev;

    @JsonProperty(CouchConstants._deleted)
    private Boolean deleted = false;

    @JsonProperty(CouchConstants._revisions)
    private Revisions revisions;

    @JsonProperty(CouchConstants._attachments)
    private Map<String, Object> attachments = new HashMap<String, Object>();

    private Map<String, Object> others = new HashMap<String, Object>();

    /**
     * Jackson will automatically put any field it can not find match to this @JsonAnySetter bucket. This is
     * effectively all the document content.
     */
    @JsonAnySetter
    public void setOthers(String name, Object value) {
        if(name.startsWith("_")) {
            // Just be defensive
            throw new RuntimeException("This is a reserved field, and should not be treated as document content.");
        }
        this.others.put(name, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getOthers() {
        return this.others;
    }

    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }

    public String getRev() {
        return rev;
    }
    public void setRev(String rev) {
        this.rev = rev;
    }

    public Revisions getRevisions() {
        return revisions;
    }
    public void setRevisions(Revisions revisions) {
        this.revisions = revisions;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public Map<String, Object> getAttachments() {
        return attachments;
    }

    public void setAttachments(Map<String, Object> attachments) {
        this.attachments = attachments;
    }

    public static class Revisions {

        private int start;
        private List<String> ids;

        public void setStart(int start) {
            this.start = start;
        }

        public int getStart() {
            return start;
        }

        public void setIds(List<String> ids) {
            this.ids = ids;
        }

        public List<String> getIds() {
            return ids;
        }

    }

    @Override
    public String toString(){
        return String.format("ID: %s rev: %s revisionIds: %s",id,rev,revisions.ids);
    }
}
