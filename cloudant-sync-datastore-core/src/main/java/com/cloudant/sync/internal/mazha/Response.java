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


/**
 * Represents CouchDB response as a result of a request.
 * @author Ahmed Yehia
 *
 * @api_private
 */
public class Response {

    private boolean ok;
	private String id;
	private String rev;

    private String error;
    private String reason;

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getId() {
		return id;
	}

	public String getRev() {
		return rev;
	}

    public boolean getOk() {
        return ok;
    }

    @Override
	public String toString() {
		return ok ? "Response [id=" + id + ", rev=" + rev + "]" : "[error: " + error + ", reason" + reason + "]";
	}
}
