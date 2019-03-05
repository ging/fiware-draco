package org.apache.nifi.processors.ngsi.ngsi.backends.hdfs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class HDFSBackendBinary implements HDFSBackend {


    private final String hdfsUser;
    private final String hdfsPassword;
    private final String oauth2Token;
    private final String hiveServerVersion;
    private final String hiveHost;
    private final String hivePort;
    private final boolean serviceAsNamespace;
    private FSGetter fsGetter;

    /**
     *
     * @param hdfsHosts
     * @param hdfsPort
     * @param hdfsUser
     * @param hdfsPassword
     * @param oauth2Token
     * @param hiveServerVersion
     * @param hiveHost
     * @param hivePort
     * @param krb5
     * @param krb5User
     * @param krb5Password
     * @param krb5LoginConfFile
     * @param krb5ConfFile
     * @param serviceAsNamespace
     */
    public HDFSBackendBinary(String[] hdfsHosts, String hdfsPort, String hdfsUser, String hdfsPassword,
                                 String oauth2Token, String hiveServerVersion, String hiveHost, String hivePort, boolean krb5,
                                 String krb5User, String krb5Password, String krb5LoginConfFile, String krb5ConfFile,
                                 boolean serviceAsNamespace) {
        this.hdfsUser = hdfsUser;
        this.hdfsPassword = hdfsPassword;
        this.oauth2Token = oauth2Token;
        this.hiveServerVersion = hiveServerVersion;
        this.hiveHost = hiveHost;
        this.hivePort = hivePort;
        this.serviceAsNamespace = serviceAsNamespace;
        this.fsGetter = new FSGetter(hdfsHosts, hdfsPort);
    } // HDFSBackendImplBinary

    protected void setFSGetter(FSGetter fsGetter) {
        this.fsGetter = fsGetter;
    } // setFSGetter

    public void createDir(String dirPath) throws Exception {
        CreateDirPEA pea = new CreateDirPEA(dirPath);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);
        ugi.doAs(pea);
    } // createDir

    public void createFile(String filePath, String data) throws Exception {
        CreateFilePEA pea = new CreateFilePEA(filePath, data);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);
        ugi.doAs(pea);
    } // createFile

    public void append(String filePath, String data) throws Exception {
        AppendPEA pea = new AppendPEA(filePath, data);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);
        ugi.doAs(pea);
    } // append

    public boolean exists(String filePath) throws Exception {
        ExistsPEA pea = new ExistsPEA(filePath);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);
        ugi.doAs(pea);
        return pea.exists();
    } // exists

    private class CreateDirPEA implements PrivilegedExceptionAction {

        private final String dirPath;

        public CreateDirPEA(String dirPath) {
            this.dirPath = dirPath;
        } // CreateDirPEA

        @Override
        public Void run() throws Exception {
            String effectiveDirPath = "/user/" + (serviceAsNamespace ? "" : (hdfsUser + "/")) + dirPath;
            FileSystem fileSystem = fsGetter.get();
            Path path = new Path(effectiveDirPath);

            if (!fileSystem.mkdirs(path)) {
                fileSystem.close();
                throw new Exception("The /user/" + (serviceAsNamespace ? "" : (hdfsUser + "/"))
                        + dirPath + " directory could not be created in HDFS");
            } // if

            fileSystem.close();
            return null;
        } // run

    } // CreateDirPEA

    private class CreateFilePEA implements PrivilegedExceptionAction {

        private final String filePath;
        private final String data;

        public CreateFilePEA(String filePath, String data) {
            this.filePath = filePath;
            this.data = data;
        } // CreateFilePEA

        @Override
        public Void run() throws Exception {
            String effectiveFilePath = "/user/" + (serviceAsNamespace ? "" : (hdfsUser + "/")) + filePath;
            FileSystem fileSystem = fsGetter.get();
            Path path = new Path(effectiveFilePath);
            FSDataOutputStream out = fileSystem.create(path);

            if (out == null) {
                fileSystem.close();
                throw new Exception("The /user/" + (serviceAsNamespace ? "" : (hdfsUser + "/"))
                        + filePath + " file could not be created in HDFS");
            } // if

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
            writer.write(data + "\n");
            writer.close();
            fileSystem.close();
            return null;
        } // run

    } // CreateFilePEA


    private class AppendPEA implements PrivilegedExceptionAction {

        private final String filePath;
        private final String data;

        public AppendPEA(String filePath, String data) {
            this.filePath = filePath;
            this.data = data;
        } // AppendPEA

        @Override
        public Void run() throws Exception {
            String effectiveDirPath = "/user/" + (serviceAsNamespace ? "" : (hdfsUser + "/")) + filePath;
            FileSystem fileSystem = fsGetter.get();
            Path path = new Path(effectiveDirPath);
            FSDataOutputStream out = fileSystem.append(path);

            if (out == null) {
                fileSystem.close();
                throw new Exception("The /user/" + (serviceAsNamespace ? "" : (hdfsUser + "/"))
                        + filePath + " file could not be created in HDFS");
            } // if

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
            writer.append(data + "\n");
            writer.close();
            fileSystem.close();
            return null;
        } // run

    } // AppendPEA


    private class ExistsPEA implements PrivilegedExceptionAction {

        private final String filePath;
        private boolean exists;

        public ExistsPEA(String filePath) {
            this.filePath = filePath;
        } // ExistsPEA

        @Override
        public Void run() throws Exception {
            String effectiveDirPath = "/user/" + (serviceAsNamespace ? "" : (hdfsUser + "/")) + filePath;
            FileSystem fileSystem = fsGetter.get();
            Path path = new Path(effectiveDirPath);
            exists = fileSystem.exists(path);
            fileSystem.close();
            return null;
        } // run

        public boolean exists() {
            return exists;
        } // exists

    } // ExistsPEA


    protected class FSGetter {

        private final String[] hdfsHosts;
        private final String hdfsPort;

        /**
         * Constructor.
         * @param hdfsHosts
         * @param hdfsPort
         */
        public FSGetter(String[] hdfsHosts, String hdfsPort) {
            this.hdfsHosts = hdfsHosts;
            this.hdfsPort = hdfsPort;
        } // FSGetter

        /**
         * Gets a Hadoop FileSystem.
         * @return
         * @throws java.io.IOException
         */
        public FileSystem get() throws IOException {
            for (String hdfsHost: hdfsHosts) {
                Configuration conf = new Configuration();
                conf.set("fs.default.name", "hdfs://" + hdfsHost + ":" + hdfsPort);
                return FileSystem.get(conf);
            } // for

            System.out.println("No HDFS file system could be got, the sink will not work!");
            return null;
        } // get

    } // FSGetter

}

