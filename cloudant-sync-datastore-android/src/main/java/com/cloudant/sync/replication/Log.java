package com.cloudant.sync.replication;

import android.app.Application;
import android.os.Environment;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Log {

    private static final String CONTROL_MSG = "LOG_FILE_CONTROL_MESSAGE";
    private static final String DEFAULT_FILENAME = "LogcatWrapperOutput.txt";
    private static Object sLock = new Object();

    private static File logFile = new File(Environment.getExternalStorageDirectory(), DEFAULT_FILENAME);

    public static void d(String tag, String message) {
        android.util.Log.d(tag, message);
        writeToFile(tag, message, "D");
    }

    public static void e(String tag, String message) {
        android.util.Log.d(tag, message);
        writeToFile(tag, message, "E");
    }

    public static void i(String tag, String message) {
        android.util.Log.i(tag, message);
        writeToFile(tag, message, "I");
    }

    /**
     * Sets/changes the file being logged to. If the filename is being changed, it logs
     * the next filename at the beginning of the original file and the old filename at the
     * beginning of the new file.
     * setLogFile(new File(Environment.getExternalStorageDirectory(), "MyLog.txt"));
     *
     * If you change the name with this method, bear in mind that if your app components stop and
     * start, you'll need to reset the filename each time your components restart, or the default
     * filename will be used. You should therefore probably call this method from
     * {@link Application#onCreate()}
     * @param file The file to log to.
     */
    public static void setLogFile(File file) {
        synchronized (sLock) {
            if (file != null && logFile != null && logFile.exists()) {
                writeToFile(CONTROL_MSG, "Log file name changed to " + file.getName(), "I");
                File oldFile = logFile;
                logFile = file;
                writeToFile(CONTROL_MSG, "Previous log file was " + oldFile.getName(), "I");
            } else {
                logFile = file;
            }
        }
    }

    private static void writeToFile(String tag, String message, String logLevel) {
        if (logFile != null) {
            synchronized (logFile) {
                if (!logFile.exists()) {
                    try {
                        logFile.createNewFile();
                    } catch (IOException e) {
                        android.util.Log.e(tag, "Unable to create file " + logFile.getName());
                        e.printStackTrace();
                    }
                }
                try {
                    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(logFile, true));
                    SimpleDateFormat sdf = new SimpleDateFormat("MM-dd HH:mm:ss.SSS", Locale.US);
                    String dateString = sdf.format(new Date());
                    String logMessage = String.format(Locale.US, "%s %s/%s :%s", dateString, logLevel, tag,
                        message);
                    bufferedWriter.append(logMessage);
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                    bufferedWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
