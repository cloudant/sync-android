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

package com.cloudant.sync.internal.util;

import com.cloudant.sync.internal.sqlite.Cursor;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @api_private
 */
public  class DatabaseUtils {

    private final static String LOG_TAG = "DatabaseUtils";
    private final static Logger logger = Logger.getLogger(DatabaseUtils.class.getCanonicalName());

    public static void closeCursorQuietly(Cursor cursor) {
        try {
            if (cursor != null) {
                cursor.close();
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE,"Error closing cursor",e);
        }
    }

    public static String makePlaceholders(int len) {
        if (len < 1) {
            // It will lead to an invalid query anyway ..
            throw new RuntimeException("No placeholders");
        } else {
            StringBuilder sb = new StringBuilder(len * 2 - 1);
            sb.append("?");
            for (int i = 1; i < len; i++) {
                sb.append(",?");
            }
            return sb.toString();
        }
    }

}
