package org.apache.nifi.processors.ngsi.ngsi.backends.hdfs;

public interface HDFSBackend {

    /**
     * Creates a directory in HDFS given its relative path. The absolute path will be built as:
     * hdfs:///user/\<hdfsUser\>/\<dirPath\>
     *
     * @param dirPath Directory to be created
     * @throws Exception
     */
    void createDir(String dirPath) throws Exception;

    /**
     * Creates a file in HDFS with initial content given its relative path. The absolute path will be build as:
     * hdfs:///user/\<hdfsUser\>/\<filePath\>
     *
     * @param filePath File to be created
     * @param data Data to be written in the created file
     * @throws Exception
     */
    void createFile(String filePath, String data) throws Exception;

    /**
     * Appends data to an existent file in HDFS.
     *
     * @param filePath File to be created
     * @param data Data to be appended in the file
     * @throws Exception
     */
    void append(String filePath, String data) throws Exception;

    /**
     * Checks if the file exists in HDFS.
     *
     * @param filePath File that must be checked
     * @return
     * @throws Exception
     */
    boolean exists(String filePath) throws Exception;

} // HDFSBackend