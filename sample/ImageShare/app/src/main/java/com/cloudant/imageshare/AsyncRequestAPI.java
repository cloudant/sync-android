package com.cloudant.imageshare;

import android.os.AsyncTask;
import android.util.Log;
import android.util.Pair;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pettyurin on 7/17/14.
 */
public class AsyncRequestAPI extends AsyncTask<String, String, String>{

    @Override
    protected String doInBackground(String... params) {
        try {
            URI uri = new URI(params[0]);
            HttpClient httpClient = new DefaultHttpClient();
            HttpPut put = new HttpPut(uri);
            put.setHeader("Content-type", "application/json");
            //List<NameValuePair> pairs = new ArrayList<NameValuePair>();
            //pairs.add(new BasicNameValuePair("db", params[1]));
            JSONObject json = new JSONObject();
            json.put("db", params[1]);
            StringEntity se = new StringEntity(json.toString());
            put.setEntity(se);
            HttpResponse response = httpClient.execute(put);
            StatusLine l = response.getStatusLine();
            if (l.getStatusCode() != 200) {
                Log.d("Status: ", l.getReasonPhrase());
                return null;
            }
            String resp_string = EntityUtils.toString(response.getEntity());
            return resp_string;
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
