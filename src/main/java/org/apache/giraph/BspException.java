package org.apache.giraph;

/**
 * Exceptions that happen in Giraph
 */
public class BspException
        extends Exception {

    /** Initial serialization UID */
    private static final long serialVersionUID = 1L;

    /** Preapplication code failed */
    public static class PreApplicationException extends BspException {
        /** Initial serialization UID */
        private static final long serialVersionUID = 1L;
    }
}
