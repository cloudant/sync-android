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

public class CouchException extends RuntimeException {

    private int statusCode;
    private String error = "Unknown error";
    private String reason = "Unknown reason";

    public CouchException() {};

    public CouchException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public CouchException(String message, Throwable cause, int statusCode) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CouchException: ");
        sb.append("error: ").append(error);
        sb.append(", reason: ").append(reason);
        sb.append(", statusCode: ").append(statusCode);
        sb.append(", msg: ").append(getMessage());
        sb.append(", cause: ").append(getCause());
        return sb.toString();
    }
}
