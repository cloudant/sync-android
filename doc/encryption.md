# Datastore encryption

Android’s datastore now supports encryption of all data inside your database 
using 256-bit AES: JSON documents, Query indexes and attachments.

JSON documents and Query indexes are stored in SQLite databases. We use
SQLCipher to encrypt the data in those databases. Attachment data is 
stored as binary blobs on the filesystem. We use the Android JCE implementation
for this, using 256-bit AES in CBC mode. Each distinct file blob gets its
own IV, stored with the file on disk.

## Using Encryption in your Application

If you plan to use encryption, first you need to download the SQLCipher
`.jar` and `.so` binary files to include them in your application.
We support version 3.2 and higher (3.2 and 3.3 tested).

Download the libraries and follow the tutorial at the [SQLCipher website][1]. We've 
included a summary below.

[1]: https://www.zetetic.net/sqlcipher/open-source/

Be sure to download version 3.2 and up; at the time or writing the page linked above
has 3.3 but the tutorial documentation is still linking to 3.1.

First add the downloaded binaries to the appropriate folders within your app structure:

1.	Add shared library files and SQLCipher jar to `jniLibs` directory under the 
    Android app directory.  [Example from sync-android AndroidTest app][2].
2.	Add required ICU zip to your app's `asset` folder.  [Example from sync-android AndroidTest app][3]:
3.	Add `sqlcipher.jar` to the Java Build Path.  This can be added in the 
    _Dependencies_ tab, which is found under the _Open Module Settings_ in 
    the Android Studio app folder context menu.

[2]: https://github.com/cloudant/sync-android/tree/feature-encryption/AndroidTest/app/src/main/jniLibs
[3]: https://github.com/cloudant/sync-android/tree/feature-encryption/AndroidTest/app/src/main/assets

Once you've included the binaries in your app's build, you need to perform some set up in code:

1.	Load SQLCipher library.  This line needs to be done before any database calls:
    
    ```java
    SQLiteDatabase.loadLibs(this);
    ```

2.	With encryption, two parameters are required in the `openDatastore` call: the 
    application storage path and a `KeyProvider` object.  The `KeyProvider` interface 
    can be instantiated with the `SimpleKeyProvider` class, which just provides a
    developer or user set encryption key.  Create a new SimpleKeyProvider 
    by providing a 256-bit key as a `byte` array in the constructor. For example:
    
    ```java
    // Security risk here: hard-coded key.
    // We recommend using java.util.SecureRandom to generate your key, then
    // storing securely or retreiving it from elsewhere. 
    // Or use AndroidKeyProvider, which does this for you.
    byte[] key = "testAKeyPasswordtestAKeyPassword".getBytes();  
    KeyProvider keyProvider = new SimpleKeyProvider(key); 

    File path = getApplicationContext().getDir("datastores", MODE_PRIVATE);
    DatastoreManager manager = DatastoreManager.getInstance(path.getAbsolutePath());

    Datastore ds = manager.openDatastore("my_datastore", keyProvider);
    ```
    
    Note: The key _must_ be 32 bytes (256-bit key). `"testAKeyPasswordtestAKeyPassword"`
    happens to meet this requirement when `getBytes()` is called, which makes this
    example more readable.
    
    See the next section for a secure key provider, which generates and protects keys
    for you.

## Secure Key Generation and Storage using AndroidKeyProvider

The SimpleKeyProvider does not provide proper management and storage of the key.  
If this is required, use the `AndroidKeyProvider`. This class handles 
generating a strong encryption key protected using the provided password. The
key data is encrytped and saved into the application's `SharedPreferences`. The 
constructor requires the Android context, a user-provided password, and an 
identifier to access the saved data in SharedPreferences.

Example:

```java
KeyProvider keyProvider = new AndroidKeyProvider(context, 
        "ASecretPassword", "AnIdentifier");
Datastore ds = manager.openDatastore("my_datastore", keyProvider);
```

One example of an identifier might be if multiple users share the same
device, the identifier can be used on a per user basis to store a key
to their individual database.

Right now, a different key is stored under each identifier, so one cannot
use identifiers to allow different users access to the same encrypted
database -- each user would get a different encryption key, and would
not be able to decrypt the database. For this use case, currently 
a custom implementation of `KeyProvider` is required.

## Full code sample:

```java
protected void onCreate(Bundle savedInstanceState) {
 
    super.onCreate(savedInstanceState);
 
    SQLiteDatabase.loadLibs(this);
 
    // Create a DatastoreManager with encryption using 
    // application internal storage path and a key
    File path = getApplicationContext().getDir("datastores", MODE_PRIVATE);
    KeyProvider keyProvider = 
            new SimpleKeyProvider("testAKeyPasswordtestAKeyPassword".getBytes());
 
    DatastoreManager manager = DatastoreManager.getInstance(path.getAbsolutePath());
 
    Datastore ds = null;
    try {
        ds = manager.openDatastore("my_datastore", keyProvider);
    } catch (DatastoreNotCreatedException e) {
        e.printStackTrace();
    }
        
    // ...
````

## Known issues

Below we list known issues and gotchas.

### Unexpected type: 5

If you use a version of SQLCipher lower than 3.2, you may see an exception saving documents.
We've observed this on version 3.1. It seems to be to do with the column type IDs reported
by SQLite/SQLCipher not matching the constants defined in SQLCipher's Java code.

The exception has the form:

    java.util.concurrent.ExecutionException: java.lang.RuntimeException: Unexpected type: 5
            at java.util.concurrent.FutureTask.report(FutureTask.java:93)
            at java.util.concurrent.FutureTask.get(FutureTask.java:163)
            at com.cloudant.sync.datastore.BasicDatastore.createDocumentFromRevision(BasicDatastore.java:1824)
            ...
            at com.android.internal.os.ZygoteInit$MethodAndArgsCaller.run(ZygoteInit.java:903)
            at com.android.internal.os.ZygoteInit.main(ZygoteInit.java:698)
     Caused by: java.lang.RuntimeException: Unexpected type: 5
            at com.cloudant.sync.datastore.BasicDatastore.getFullRevisionFromCurrentCursor(BasicDatastore.java:1709)
            at com.cloudant.sync.datastore.BasicDatastore.getDocumentInQueue(BasicDatastore.java:284)
            at com.cloudant.sync.datastore.BasicDatastore.createDocument(BasicDatastore.java:681)
            at com.cloudant.sync.datastore.BasicDatastore.access$1500(BasicDatastore.java:65)
            at com.cloudant.sync.datastore.BasicDatastore$23.call(BasicDatastore.java:1816)
            at com.cloudant.sync.datastore.BasicDatastore$23.call(BasicDatastore.java:1812)
            at SQLQueueCallable.call(SQLQueueCallable.java:34)
            at java.util.concurrent.FutureTask.run(FutureTask.java:237)
            at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1112)
            at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:587)
            at java.lang.Thread.run(Thread.java:818)

To fix this, make sure to download 3.2 or higher of SQLCipher's Android library code from
https://www.zetetic.net/sqlcipher/open-source/. The Android documentation on the SQLCipher
site links to 3.1 right now.

## Licence

We use [JCE][JCE] library to encrypt the attachments before
saving to disk. There should be no licencing concerns for using JCE.

Databases are automatically encrypted with
[SQLCipher][SQLCipher]. SQLCipher requires including its
[BSD-style license][BSD-style license] and copyright in your application and
documentation. Therefore, if you use datastore encryption in your application, 
please follow the instructions mentioned [here](https://www.zetetic.net/sqlcipher/open-source/).

[SQLCipher]: https://www.zetetic.net/sqlcipher/
[JCE]: http://developer.android.com/reference/javax/crypto/package-summary.html
[BSD-style license]:https://www.zetetic.net/sqlcipher/license/
