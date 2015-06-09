/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.klarna.hiverunner;

import com.klarna.reflection.ReflectionUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
//import org.apache.hadoop.hive.shims.Hadoop23Shims;

import org.apache.hadoop.hive.shims.Hadoop23Shims;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.hsqldb.jdbc.JDBCDriver;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.UUID;

import javax.security.auth.login.LoginException;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.*;

/**
 * Configuration for running the HiveServer within this JVM with zero external dependencies.
 * <p/>
 * This class contains a bunch of methods meant to be overridden in order to create slightly different contexts.
 */
class StandaloneHiveServerContext implements HiveServerContext {

    private String metaStorageUrl;

    private HiveConf hiveConf = new HiveConf();

    private TemporaryFolder basedir;

    StandaloneHiveServerContext(TemporaryFolder basedir) {
        this.basedir = basedir;

        this.metaStorageUrl = "jdbc:hsqldb:mem:" + UUID.randomUUID().toString();

        hiveConf.setBoolVar(HIVESTATSAUTOGATHER, false);

        // Set the hsqldb driver. datanucleus will
        hiveConf.set("datanucleus.connectiondrivername", "org.hsqldb.jdbc.JDBCDriver");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.hsqldb.jdbc.JDBCDriver");

        // No pooling needed. This will save us a lot of threads
        hiveConf.set("datanucleus.connectionPoolingType", "None");

        // Defaults to a 1000 millis sleep in
        // org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper.
        hiveConf.setLongVar(HiveConf.ConfVars.HIVECOUNTERSPULLINTERVAL, 1L);

        hiveConf.setVar(HADOOPBIN, "NO_BIN!");

        try {
            Class.forName(JDBCDriver.class.getName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }


        configureJavaSecurityRealm(hiveConf);

        configureJobTrackerMode(hiveConf);

        configureSupportConcurrency(hiveConf);

        configureFileSystem(basedir, hiveConf);

        configureMetaStoreValidation(hiveConf);

        configureMapReduceOptimizations(hiveConf);

        configureCheckForDefaultDb(hiveConf);

        configureAssertionStatus(hiveConf);
    }

    protected void configureJavaSecurityRealm(HiveConf hiveConf) {
        // These two properties gets rid of: 'Unable to load realm info from SCDynamicStore'
        // which seems to have a timeout of about 5 secs.
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");
    }

    protected void configureAssertionStatus(HiveConf conf) {
        ClassLoader.getSystemClassLoader().setPackageAssertionStatus("org.apache.hadoop.hive.serde2.objectinspector",
                false);
    }

    protected void configureCheckForDefaultDb(HiveConf conf) {
        hiveConf.setBoolean("hive.metastore.checkForDefaultDb", true);
    }

    protected void configureSupportConcurrency(HiveConf conf) {
        hiveConf.setBoolVar(HIVE_SUPPORT_CONCURRENCY, false);
    }

    protected void configureMetaStoreValidation(HiveConf conf) {
        conf.setBoolVar(METASTORE_VALIDATE_CONSTRAINTS, true);
        conf.setBoolVar(METASTORE_VALIDATE_COLUMNS, true);
        conf.setBoolVar(METASTORE_VALIDATE_TABLES, true);
    }

    protected void configureJobTrackerMode(HiveConf conf) {
        /*
        * Overload shims to make sure that org.apache.hadoop.hive.ql.exec.MapRedTask#runningViaChild
         * validates to false.
         *
         * Search for usage of org.apache.hadoop.hive.shims.HadoopShims#isLocalMode to find other affects of this.
        */
        ReflectionUtils.setStaticField(ShimLoader.class, "hadoopShims", new HadoopShims() {
            @Override
        	public boolean isLocalMode(Configuration conf) {
            	
            	
                return true;
            }

			@Override
			public String addServiceToToken(String arg0, String arg1)
					throws IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void authorizeProxyAccess(String arg0,
					UserGroupInformation arg1, String arg2, Configuration arg3)
					throws IOException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void closeAllForUGI(UserGroupInformation arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public int compareText(Text arg0, Text arg1) {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public Path createDelegationTokenFile(Configuration arg0)
					throws IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public int createHadoopArchive(Configuration arg0, Path arg1,
					Path arg2, String arg3) throws Exception {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public FileSystem createProxyFileSystem(FileSystem arg0, URI arg1) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public UserGroupInformation createProxyUser(String arg0)
					throws IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public UserGroupInformation createRemoteUser(String arg0,
					List<String> arg1) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public <T> T doAs(UserGroupInformation arg0,
					PrivilegedExceptionAction<T> arg1) throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public boolean fileSystemDeleteOnExit(FileSystem arg0, Path arg1)
					throws IOException {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public long getAccessTime(FileStatus arg0) {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public CombineFileInputFormatShim getCombineFileInputFormat() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public long getDefaultBlockSize(FileSystem arg0, Path arg1) {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public short getDefaultReplication(FileSystem arg0, Path arg1) {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public HdfsFileStatus getFullFileStatus(Configuration arg0,
					FileSystem arg1, Path arg2) throws IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public HCatHadoopShims getHCatShim() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public URI getHarUri(URI arg0, URI arg1, URI arg2)
					throws URISyntaxException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getInputFormatClassName() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getJobLauncherHttpAddress(Configuration arg0) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getJobLauncherRpcAddress(Configuration arg0) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public JobTrackerState getJobTrackerState(ClusterStatus arg0)
					throws Exception {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getKerberosShortName(String arg0) throws IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getMRFramework(Configuration arg0) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public MiniDFSShim getMiniDfs(Configuration arg0, int arg1,
					boolean arg2, String[] arg3) throws IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public MiniMrShim getMiniMrCluster(Configuration arg0, int arg1,
					String arg2, int arg3) throws IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getShortUserName(UserGroupInformation arg0) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getTaskAttemptLogUrl(JobConf arg0, String arg1,
					String arg2) throws MalformedURLException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String[] getTaskJobIDs(TaskCompletionEvent arg0) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getTokenFileLocEnvName() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getTokenStrForm(String arg0) throws IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public UserGroupInformation getUGIForConf(Configuration arg0)
					throws LoginException, IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public WebHCatJTShim getWebHCatShim(Configuration arg0,
					UserGroupInformation arg1) throws IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void inputFormatValidateInput(InputFormat arg0, JobConf arg1)
					throws IOException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public boolean isJobPreparing(RunningJob arg0) throws IOException {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean isLoginKeytabBased() throws IOException {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean isSecureShimImpl() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean isSecurityEnabled() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public void loginUserFromKeytab(String arg0, String arg1)
					throws IOException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public boolean moveToAppropriateTrash(FileSystem arg0, Path arg1,
					Configuration arg2) throws IOException {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public JobContext newJobContext(Job arg0) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public TaskAttemptContext newTaskAttemptContext(Configuration arg0,
					Progressable arg1) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public TaskAttemptID newTaskAttemptID(JobID arg0, boolean arg1,
					int arg2, int arg3) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void prepareJobOutput(JobConf arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void reLoginUserFromKeytab() throws IOException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void refreshDefaultQueue(Configuration arg0, String arg1)
					throws IOException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void setFloatConf(Configuration arg0, String arg1, float arg2) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void setFullFileStatus(Configuration arg0,
					HdfsFileStatus arg1, FileSystem arg2, Path arg3)
					throws IOException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void setJobLauncherRpcAddress(Configuration arg0, String arg1) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void setMRFramework(Configuration arg0, String arg1) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void setTmpFiles(String arg0, String arg1) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void setTokenStr(UserGroupInformation arg0, String arg1,
					String arg2) throws IOException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void setTotalOrderPartitionFile(JobConf arg0, Path arg1) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public String unquoteHtmlChars(String arg0) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public boolean usesJobShell() {
				// TODO Auto-generated method stub
				return false;
			}
        });
    }

    protected void configureFileSystem(TemporaryFolder basedir, HiveConf conf) {
        conf.setVar(METASTORECONNECTURLKEY, metaStorageUrl + ";create=true");

        createAndSetFolderProperty(METASTOREWAREHOUSE, "warehouse", conf, basedir);
        createAndSetFolderProperty(SCRATCHDIR, "scratchdir", conf, basedir);
        createAndSetFolderProperty(LOCALSCRATCHDIR, "localscratchdir", conf, basedir);
        createAndSetFolderProperty(METASTOREDIRECTORY, "metastore", conf, basedir);
        createAndSetFolderProperty(HIVEHISTORYFILELOC, "tmp", conf, basedir);

        conf.setBoolVar(HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS, true);

        createAndSetFolderProperty("hadoop.tmp.dir", "hadooptmp", conf, basedir);
        createAndSetFolderProperty("test.log.dir", "logs", conf, basedir);
        createAndSetFolderProperty("hive.vs", "vs", conf, basedir);
    }

    private File newFolder(TemporaryFolder basedir, String folder) {
        try {
            return basedir.newFolder(folder);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create tmp dir: " + e.getMessage(), e);
        }
    }

    private File newFile(TemporaryFolder basedir, String fileName) {
        try {
            return basedir.newFile(fileName);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create tmp file: " + e.getMessage(), e);
        }
    }

    protected void configureMapReduceOptimizations(HiveConf conf) {
        /*
        * Switch off all optimizers otherwise we didn't
        * manage to contain the map reduction within this JVM.
        */
        conf.setBoolVar(HIVE_INFER_BUCKET_SORT, false);
        conf.setBoolVar(HIVEMETADATAONLYQUERIES, false);
        conf.setBoolVar(HIVEOPTINDEXFILTER, false);
        conf.setBoolVar(HIVECONVERTJOIN, false);
        conf.setBoolVar(HIVESKEWJOIN, false);
    }

    @Override
    public String getMetaStoreUrl() {
        return metaStorageUrl;
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }

    @Override
    public TemporaryFolder getBaseDir() {
        return basedir;
    }

    protected final void createAndSetFolderProperty(HiveConf.ConfVars var, String folder, HiveConf conf,
                                                    TemporaryFolder basedir) {
        conf.setVar(var, newFolder(basedir, folder).getAbsolutePath());
    }

    protected final void createAndSetFolderProperty(String key, String folder, HiveConf conf, TemporaryFolder basedir) {
        conf.set(key, newFolder(basedir, folder).getAbsolutePath());
    }


}
