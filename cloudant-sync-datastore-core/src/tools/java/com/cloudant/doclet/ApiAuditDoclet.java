/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.doclet;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.RootDoc;
import com.sun.javadoc.Tag;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by tomblench on 21/04/16.
 */
public class ApiAuditDoclet {

    /**
     * Produce a listing of classes grouped by API status (public/private)
     * @param root The root of the program structure information for one run of javadoc
     * @return true - no error handling
     *
     * @api_private
     */
    public static boolean start(RootDoc root) {
        TreeMap<String, String> classCategories = new TreeMap<String, String>();
        for (ClassDoc cd : root.classes()) {
            ClassDoc maybeContaining = cd;
            // if not tagged, go up chain of containing classes if we're an inner class
            while (!validTags(maybeContaining.tags()) && maybeContaining.containingClass() != null) {
                maybeContaining = maybeContaining.containingClass();
            }
            // initially set to uncategorised
            classCategories.put(cd.toString(), "(uncategorised)");
            // note this won't catch those which are under both categories
            for (Tag t : maybeContaining.tags()) {
                if (validTag(t.name())) {
                    classCategories.put(cd.toString(), t.name());
                }
            }
        }
        TreeMap<String, List<String>> categoryClasses = new TreeMap<String, List<String>>();
        // reverse hash
        for (Map.Entry<String, String> e:  classCategories.entrySet())
        {
            String cat = e.getValue();
            String theClass = e.getKey();
            if (!categoryClasses.containsKey(cat)) {
                categoryClasses.put(cat, new ArrayList<String>());
            }
            categoryClasses.get(cat).add(theClass);
        }
        for (Map.Entry<String, List<String>> e:  categoryClasses.entrySet()) {
            System.out.println("*** "+e.getKey()+" ***");
            e.getValue().sort(String.CASE_INSENSITIVE_ORDER);
            for (String theClass : e.getValue()) {
                System.out.println(theClass);
            }
        }
        return true;
    }

    private static boolean validTags(Tag[] tags) {
        for (Tag t : tags) {
            if (validTag(t.name())) {
                return true;
            }
        }
        return false;
    }
        
    private static boolean validTag(String tag)
    {
        return (tag.equals("@api_private") ||
                tag.equals("@api_public"));
    }
    
    public static int optionLength(String option)
    {
        // we ignore these options anyway
        if (option.equals("-d") || option.equals("-doctitle") || option.equals("-windowtitle")) {
            return 2;
        }
        return 0;
    }

}
