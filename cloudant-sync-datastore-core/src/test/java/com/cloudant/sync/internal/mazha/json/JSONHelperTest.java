/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
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

package com.cloudant.sync.internal.mazha.json;

import com.cloudant.sync.internal.mazha.Response;
import com.cloudant.sync.internal.util.JSONUtils;
import com.cloudant.sync.util.TestUtils;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class JSONHelperTest {

    private static Map<String, Object> expectedJSONMap;
    private static String expectedJson;


    @BeforeClass
    public static void beforeClass() throws Exception {
        expectedJSONMap = new TreeMap<String, Object>();


        expectedJSONMap.put("Sunrise", true);
        expectedJSONMap.put("Sunset", false);
        expectedJSONMap.put("Data", "A run to the head of the blood");

        TreeMap<String,Object> organizer = new TreeMap<String, Object>();
        organizer.put("Name", "Tom");
        organizer.put( "Sex", "Male");
        expectedJSONMap.put("Organizer", organizer);


        ArrayList<Number> fullhours = new ArrayList<Number>();
        fullhours.add(1);
        fullhours.add(2);
        fullhours.add(3);
        fullhours.add(4);
        fullhours.add(5);
        fullhours.add(6);
        fullhours.add(7);
        fullhours.add(8);
        fullhours.add(9);
        fullhours.add(10);

        expectedJSONMap.put("FullHours", fullhours);

        TreeMap<String,Object> football = new TreeMap<String, Object>();
        football.put("Name", "Football");
        football.put("Duration", 2);
        football.put("DurationUnit", "Hours");

        TreeMap<String,Object> breakfast = new TreeMap<String, Object>();
        breakfast.put("Name","Breakfast");
        breakfast.put("Duration", 40);
        breakfast.put("DurationUnit", "Minutes");

        ArrayList<String> attendees = new ArrayList<String>();
        attendees.add("Jan");
        attendees.add("Damien");
        attendees.add("Laura");
        attendees.add("Gwendolyn");
        attendees.add("Roseanna");
        breakfast.put("Attendees",attendees);

        ArrayList<TreeMap> activities = new ArrayList<TreeMap>();
        activities.add(football);
        activities.add(breakfast);

        expectedJSONMap.put("Activities", activities);


        expectedJson = FileUtils.readFileToString(
                TestUtils.loadFixture("fixture/json_helper_compacted.json")
        );

    }



    @Test
    public void testHashMapToJSONString(){
        String jsonString = JSONUtils.toJson(expectedJSONMap);
        Assert.assertEquals("JSON Strings not the same",expectedJson,jsonString);

    }


    @Test
    public void testMapFromJson() throws Exception
    {
        File fixture = TestUtils.loadFixture("fixture/json_helper.json");
        FileReader fr = new FileReader(fixture);
        Map<String, Object> objectHashMap = JSONUtils.fromJson(fr);

        Assert.assertTrue("Map read is not equal to expected map",
                expectedJSONMap.equals(objectHashMap));

    }

    @Test
    public void testResponseFromJSONWithClass() throws Exception
    {

        File fixture = TestUtils.loadFixture("fixture/json_helper_response.json");
        FileReader fr = new FileReader(fixture);
        Response res = JSONUtils.fromJson(fr, Response.class);
        Assert.assertTrue("Response should have been okay -> true", res.getOk());
        Assert.assertEquals("Response should have Id of myId","myId", res.getId());
        Assert.assertEquals("Response should have rev of 1-IPromiseIamARevision",
                "1-IPromiseIamARevision",
                res.getRev());
        Assert.assertNull("Error should have been null", res.getError());
        Assert.assertNull("Reason should have been null", res.getReason());
    }

    @Test
    public void testListOfResponsesFromJSONWithTypeReference() throws Exception
    {

        //Note json in helper json is partial, it only contains an array of reponses
        File fixture = TestUtils.loadFixture("fixture/json_helper_response_list.json");
        FileReader fr = new FileReader(fixture);
        List<Response> responses = JSONUtils.fromJson(fr, new TypeReference<List<Response>>() {});

        Assert.assertEquals("Number of responses is incorrect", 2, responses.size());
        Response res = responses.get(0);
        Assert.assertTrue("Response should have been okay -> true", res.getOk());
        Assert.assertEquals("Response should have Id of myId", "myId", res.getId());
        Assert.assertEquals("Response should have rev of 1-IPromiseIamARevision",
                "1-IPromiseIamARevision",
                res.getRev());
        Assert.assertNull("Error should have been null", res.getError());
        Assert.assertNull("Reason should have been null", res.getReason());

        res = responses.get(1);
        Assert.assertFalse("Response should have been okay -> false", res.getOk());
        Assert.assertNull("Response should have Id of null", res.getId());
        Assert.assertNull("Response should have rev of null", res.getRev());
        Assert.assertEquals("Error should have been 403", "403", res.getError());
        Assert.assertEquals("Reason should have been Forbidden", "Forbidden", res.getReason());

    }

}
