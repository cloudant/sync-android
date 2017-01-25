/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
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


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @api_private
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class CouchDbInfo {
	
	@JsonProperty("db_name")
	private String dbName;

	@JsonProperty("doc_count")
	private long docCount;

	@JsonProperty("doc_del_count")
	private String docDelCount;

	@JsonProperty("update_seq")
	private Object updateSeq;

	@JsonProperty("purge_seq")
	private long purgeSeq;

	@JsonProperty("compact_running")
	private boolean compactRunning;

	@JsonProperty("disk_size")
	private long diskSize;

	@JsonProperty("instance_start_time")
	private long instanceStartTime;

	@JsonProperty("disk_format_version")
	private int diskFormatVersion;

    @JsonProperty("data_size")
    private long dataSize;

	public String getDbName() {
		return dbName;
	}

	public long getDocCount() {
		return docCount;
	}

	public String getDocDelCount() {
		return docDelCount;
	}

	public Object getUpdateSeq() {
		return updateSeq;
	}

	public long getPurgeSeq() {
		return purgeSeq;
	}

	public boolean isCompactRunning() {
		return compactRunning;
	}

	public long getDiskSize() {
		return diskSize;
	}

	public long getInstanceStartTime() {
		return instanceStartTime;
	}

	public int getDiskFormatVersion() {
		return diskFormatVersion;
	}

    public long getDataSize() {
        return dataSize;
    }
    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    @Override
	public String toString() {
		return String
				.format("CouchDbInfo [dbName=%s, docCount=%s, docDelCount=%s, updateSeq=%s, purgeSeq=%s, compactRunning=%s, diskSize=%s, instanceStartTime=%s, diskFormatVersion=%s, dataSize=%s]",
						dbName, docCount, docDelCount, updateSeq, purgeSeq,
						compactRunning, diskSize, instanceStartTime,
						diskFormatVersion, dataSize);
	}
}
