/*
 * IBM Confidential OCO Source Materials
 * 
 * 5725-I43 Copyright IBM Corp. 2006, 2013
 * 
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has
 * been deposited with the U.S. Copyright Office.
 * 
 */

/* Copyright (C) Worklight Ltd. 2006-2012.  All rights reserved. */

package com.cloudant.sync.sqlite.android.encryption;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.Matrix;
import android.graphics.drawable.BitmapDrawable;
import android.graphics.drawable.Drawable;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.ResourceBundle;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 *
 */
public class EncryptionUtils {
	//private static final Log logger = Log.(EncryptionUtils.class.getName());
     private static final String TAG = EncryptionUtils.class.getName();

     public static final int ANDROID_BUFFER_8K = 8192;

     // keep track of which libs are already loaded so we don't process multiple calls
     // to loadLib method unecessarily.
     private static HashSet<String> LOADED_LIBS = new HashSet<String>();

	private static ResourceBundle bundle;

     public static Drawable scaleImage(Drawable drawable, float scaleWidth, float scaleHeight) {
          Drawable resizedDrawable = null;
          if (drawable != null) {
               Bitmap bitmapOrg = ((BitmapDrawable) drawable).getBitmap();
               int width = bitmapOrg.getWidth();
               int height = bitmapOrg.getHeight();
               // create a matrix for the manipulation
               Matrix matrix = new Matrix();
               // resize the bit map
               matrix.postScale(scaleWidth, scaleHeight);
               Bitmap resizedBitmap = Bitmap.createBitmap(bitmapOrg, 0, 0, width, height, matrix, true);
               // make a Drawable from Bitmap to allow to set the BitMap
               resizedDrawable = new BitmapDrawable(resizedBitmap);
          }
          return resizedDrawable;
     }


     public static String getResourceString(String recourceName, Context context) {
    	 return getResourceString(recourceName, null, context);
     }
     
     public static String getResourceString(String recourceName, String argument, Context context) {
          @SuppressWarnings("rawtypes")
          // R$string class with reflection 
          Class rStringClass = null;

          try {
               if (rStringClass == null) {
                    rStringClass = Class.forName(context.getPackageName() + ".R$string");
               }               
               Integer resourceId = (Integer) rStringClass.getDeclaredField(recourceName).get(null);
               if (argument == null)
            	   return context.getResources ().getString (resourceId);
               else
            	   return context.getResources ().getString (resourceId, argument);
          } catch (Exception e) {
               Log.e(TAG, e.getMessage());
               return "";
          }
     }


     /**
      * copy input stream to output stream
      * @param in The {@link InputStream} object to be copied from.
      * @param out The {@link OutputStream} object to write to.
      * @throws IOException in case copy fails.
      */
     public static void copyFile(InputStream in, OutputStream out) throws IOException {
          // 8k is the suggest buffer size in android, do not change this
          byte[] buffer = new byte[ANDROID_BUFFER_8K];
          int read;
          while ((read = in.read(buffer)) != -1) {
               out.write(buffer, 0, read);
          }
          out.flush();
     }



     /**
      * Convert JSON string to JSONObject
      * @param jsonString - The JSON String to be converted.
      * @return the converted JSON object.
      * @throws JSONException - in case convert fails.
      */
     public static final JSONObject convertStringToJSON(String jsonString) throws JSONException {
          int beginIndex = jsonString.indexOf("{");
          int endIndex = jsonString.lastIndexOf("}");
          
          if (beginIndex == -1 || endIndex == -1 || beginIndex > endIndex + 1) {
               String message = "Input string does not contain brackets, or input string is invalid. The string is: " + jsonString;
               //logger.debug (message);
               throw new JSONException(message);
          }
          
          String secureJSONString = jsonString.substring(beginIndex, endIndex + 1);
          JSONObject jsonObject = new JSONObject(secureJSONString);
          return jsonObject;
     }

     /**
      * Convert JSONArray to List<String>
      * @param jsonArray
      * @return - the converted  List<String>
      */
     public static final List<String> convertJSONArrayToList(JSONArray jsonArray) {
          List<String> listToReturn = new ArrayList<String>();
          for (int i = 0; i < jsonArray.length(); i++) {
               try {
                    listToReturn.add((String) jsonArray.get(i));
               } catch (JSONException e) {
                    throw new RuntimeException(e);
               }
          }
          return listToReturn;
     }

     public static final byte[] hexStringToByteArray(String s) {
          int len = s.length();
          byte[] data = new byte[len / 2];
          for (int i = 0; i < len; i += 2) {
               data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
          }
          return data;
     }

     public static final String byteArrayToHexString(byte[] bytes) {  
          StringBuilder sb = new StringBuilder(bytes.length * 2);  

          Formatter formatter = new Formatter(sb);  
          for (byte b : bytes) {  
               formatter.format("%02x", b);  
          }  
          formatter.close ();
          return sb.toString();  
     }


     /**
      * This method assumes it will find the library at:
      *     files/featurelibs/{arch}/{library}.zip
      *     
      * It will unzip the library to the root folder, and delete the original,
      * then see if any other architecture folders exist and delete them since
      * they will never be used on this architecture.
      * 
      * @param context Android context
      * @param library example "libcrypto.so.1.0.0"
      * 
      * 
      */
     public static final synchronized void loadLib(Context context, String library) {
          
          // keep track of which libs are already loaded, so we don't process multiple calls for the same lib unnecessarily
          // Notice we use a static.  This means calls to loadLib for the same 'library' parameter will be processed
          // only upon app startup, not app foreground.  We want to keep the behavior for cases where the native app has been
          // updated (through the Play Store, for example) and the target .so file needs to be replaced.
          
          if (!LOADED_LIBS.contains(library)) {

               // we only support "armeabi" and "x86"
               final String ARMEABI = "armeabi";
               final String X86 = "x86";

               String arch = System.getProperty("os.arch");  // the architecture we're running on
               String nonArch = null;  // the architecture we are NOT on
               if (arch != null && arch.toLowerCase().startsWith("i")) {  // i686
                    arch = X86;
                    nonArch = ARMEABI;
               } else {
                    arch = ARMEABI;
                    nonArch = X86;
               }

               final String libPath = "featurelibs" + File.separator +  arch + File.separator + library;
               File sourceLocation = new File(context.getFilesDir(), libPath + ".zip");

               // recursively delete the architecture folder that will never be used:
               File nonArchStorage = new File(context.getFilesDir(), "featurelibs" + File.separator + nonArch);
               deleteDirectory(nonArchStorage);

               File targetFile = new File(context.getFilesDir(), library);

               // delete the target
               targetFile.delete();

               Log.d(TAG,"Extracting zip file: " + libPath);
               try{
                    InputStream istr = context.getAssets().open(libPath + ".zip");
                    unpack(istr, targetFile.getParentFile());
               }
               catch(IOException e){
            	   e.printStackTrace(); 
            	   Log.d(TAG,"Error extracting zip file: " + e.getMessage());
               }

               Log.d(TAG,"Loading library using System.load: " + targetFile.getAbsolutePath());

               // delete the original zip, which is now extracted:
               //sourceLocation.delete();

               System.load(targetFile.getAbsolutePath());
               
               LOADED_LIBS.add (library);

               //Load context in security manager
               SecurityManager.getInstance(context);
          }
     }

     /**
      * Delete a file or directory, including all its children.  The method name "deleteDirectory"
      * is retained for legacy callers, but can take a directory or file as a parameter.
      *
      * @param fileOrDirectory The {@link File} object represents the directory to delete.
      * @return true if the directory was deleted, false otherwise.
      */
     public static boolean deleteDirectory(File fileOrDirectory) {
          if (fileOrDirectory.isDirectory()) {
               for (File child : fileOrDirectory.listFiles()) {
                    deleteDirectory(child);
               }
          }
          return fileOrDirectory.delete();
     }


     public static void unpack(InputStream in, File targetDir) throws IOException {
          ZipInputStream zin = new ZipInputStream(in);

          ZipEntry entry;
          while ((entry = zin.getNextEntry()) != null) {
               String extractFilePath = entry.getName();
               if (extractFilePath.startsWith("/") || extractFilePath.startsWith("\\")) {
                    extractFilePath = extractFilePath.substring(1);
               }
               File extractFile = new File(targetDir.getPath() + File.separator + extractFilePath);
               if (entry.isDirectory()) {
                    if (!extractFile.exists()) {
                         extractFile.mkdirs();
                    }
                    continue;
               } else {
                    File parent = extractFile.getParentFile();
                    if (!parent.exists()) {
                         parent.mkdirs();
                    }
               }

               // if not directory instead of the previous check and continue
               OutputStream os = new BufferedOutputStream(new FileOutputStream(extractFile));
               copyFile(zin, os);
               os.flush();
               os.close();
          }
     }



}
