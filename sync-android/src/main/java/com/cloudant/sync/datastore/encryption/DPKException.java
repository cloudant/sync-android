package com.cloudant.sync.datastore.encryption;

/**
 * Exception thrown when encryption or decryption of Data Protect Key fails.
 */
class DPKException extends RuntimeException {

    /**
     * @param description Context of failure
     * @param cause       Root cause of failure
     */
    public DPKException(String description, Throwable cause) {
        super(description, cause);
    }
}
