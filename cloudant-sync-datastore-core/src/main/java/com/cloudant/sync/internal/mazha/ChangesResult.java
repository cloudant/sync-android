/*
 * Copyright © 2013 Cloudant
 *
 * Copyright © 2011 Ahmed Yehia (ahmed.yehia.m@gmail.com)
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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Object representation of changes feed, example:
 *
 * {
 *   "last_seq": 35,
 *   "results": [
 *     { "changes":
 *       [ { "rev": "1-bd42b942b8b672f0289cf3cd1f67044c" } ],
 *       "id": "2013-09-23T20:50:56.251Z",
 *       "seq": 27
 *     },
 *     { "changes":
 *       [ { "rev": "29-3f4dabfb32290e557ac1d16b2e8f069c" },
 *         { "rev": "29-01fcbf8a3f1457eff21e18f7766d3b45" },
 *         { "rev": "26-30722da17ad35cf1860f126dba391d67" }
 *       ],
 *       "id": "2013-09-10T17:47:17.770Z",
 *       "seq": 35
 *       }
 *    ]
 * }
 *
 * @api_private
*/
public class ChangesResult {
	private List<Row> results;

	@JsonProperty("last_seq")
	private Object lastSeq;

	public List<Row> getResults() {
		return results;
	}

	public void setResults(List<Row> results) {
		this.results = results;
	}

	public Object getLastSeq() {
		return lastSeq;
	}

	public void setLastSeq(Object lastSeq) {
		this.lastSeq = lastSeq;
	}

    /**
     * this.getResults().size()
     */
    public int size() {
        return this.getResults() == null ? 0 : this.getResults().size();
    }

	/**
	 * Represent a row in Changes result.
	 */
	public static class Row {
		private Object seq;
		private String id;
		private List<Rev> changes;
		private boolean deleted;
		private Map doc;

		public Object getSeq() {
			return seq;
		}

		public void setSeq(Object seq) {
			this.seq = seq;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public List<Rev> getChanges() {
			return changes;
		}

		public void setChanges(List<Rev> changes) {
			this.changes = changes;
		}

		public boolean isDeleted() {
			return deleted;
		}

		public void setDeleted(boolean deleted) {
			this.deleted = deleted;
		}

		public Map getDoc() {
			return doc;
		}

		public void setDoc(Map doc) {
			this.doc = doc;
		}

		/**
		 * Represent a Change rev. 
		 */
		public static class Rev {
			private String rev;

			public String getRev() {
				return rev;
			}

			public void setRev(String rev) {
				this.rev = rev;
			}
		} // end class Rev
	} // end class Row
} // end class ChangesResult
