package com.cloudant.imageshare;

import android.content.Context;
import android.content.CursorLoader;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.provider.MediaStore;
import android.util.Log;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.GridView;
import android.widget.ImageView;

import com.cloudant.sync.datastore.Attachment;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class ImageAdapter extends BaseAdapter{

    private Context mContext;
    //layout size
    private int lSize;
    private ArrayList<Bitmap> mThumbBitmaps;

    public ImageAdapter(Context c, int layoutParam) {
        mContext = c;
        lSize = layoutParam;
        mThumbBitmaps = new ArrayList<Bitmap>();
    }

    public int getCount() {
        return mThumbBitmaps.size();
    }

    public Object getItem(int position) {
        return null;
    }

    public long getItemId(int position) {
        return 0;
    }

    // create a new ImageView for each item referenced by the Adapter
    public View getView(int position, View convertView, ViewGroup parent) {
        ImageView imageView;
        if (convertView == null) {  // if it's not recycled, initialize some attributes
            imageView = new ImageView(mContext);
            imageView.setLayoutParams(new GridView.LayoutParams(lSize, lSize));
            imageView.setScaleType(ImageView.ScaleType.CENTER_CROP);
            imageView.setPadding(8, 8, 8, 8);
        } else {
            imageView = (ImageView) convertView;
        }
        imageView.setImageBitmap(mThumbBitmaps.get(position));
        return imageView;
    }

    public void addImage(Uri imageUri, Context c) throws IOException{
        Bitmap bitmap = loadBitmap(imageUri, c);
        mThumbBitmaps.add(bitmap);
    }

    public void addImage(Attachment a) throws IOException{
        Bitmap bitmap = loadBitmap(a);
        mThumbBitmaps.add(bitmap);
    }

    public void clearImageData(){
        mThumbBitmaps.clear();
    }

    public Bitmap loadBitmap(Uri imageUri, Context c) throws IOException{
        BitmapFactory.Options options = new BitmapFactory.Options();
        options.inJustDecodeBounds = true;
        BitmapFactory.decodeStream(c.getContentResolver().openInputStream(imageUri), null, options);

        options.inSampleSize = calculateSampleSize(options, lSize,lSize);

        options.inJustDecodeBounds = false;
        return BitmapFactory.decodeStream(c.getContentResolver().openInputStream(imageUri), null, options);
    }

    public Bitmap loadBitmap(Attachment a) throws IOException{
        BitmapFactory.Options options = new BitmapFactory.Options();
        options.inJustDecodeBounds = true;
        BitmapFactory.decodeStream(a.getInputStream(), null, options);

        options.inSampleSize = calculateSampleSize(options, lSize,lSize);

        options.inJustDecodeBounds = false;
        return BitmapFactory.decodeStream(a.getInputStream(), null, options);
    }

    // Decode large bitmaps efficiently
    public static int calculateSampleSize( BitmapFactory.Options options,
                                           int reqWidth, int reqHeight) {
        // Raw height and width of image
        final int height = options.outHeight;
        final int width = options.outWidth;
        int inSampleSize = 1;

        if (height > reqHeight || width > reqWidth) {

            final int halfHeight = height / 2;
            final int halfWidth = width / 2;

            // Calculate the largest inSampleSize value that is a power of 2 and keeps both
            // height and width larger than the requested height and width.
            while ((halfHeight / inSampleSize) > reqHeight
                    && (halfWidth / inSampleSize) > reqWidth) {
                inSampleSize *= 2;
            }
        }
        return inSampleSize;
    }
}
