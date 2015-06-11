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

[1]: https://www.zetetic.net/sqlcipher/sqlcipher-for-android/

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
    DatastoreManager manager = new DatastoreManager(path.getAbsolutePath()); 

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
 
    DatastoreManager manager = new DatastoreManager(path.getAbsolutePath());
 
    Datastore ds = null;
    try {
        ds = manager.openDatastore("my_datastore", keyProvider);
    } catch (DatastoreNotCreatedException e) {
        e.printStackTrace();
    }
        
    // ...
````

