/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Testcase for {@link RawParascaleFileSystem}.
 */
public class TestRawParascaleFileSystemBase extends ParascaleFsTestCase
{

	RawParascaleFileSystem fs;

	UserGroupInformation groupInformation;

	@Override
	protected void setUp() throws Exception
	{
		super.setUp();

		init();
		rmMountDir();
		assertTrue("Failed to create " + getMountDir(), mkMountDir());
		assertTrue(fs.createHomeDirectory());
	}

	protected void init() throws URISyntaxException, IOException
	{
		groupInformation = UserGroupInformation.createRemoteUser("hadoop");
		fs = getFileSystem(groupInformation);
		final Configuration conf = getConf();
		fs.initialize(new URI(conf.get(FS_DEFAULT_NAME)), getConf());
	}

	@Override
	protected void tearDown() throws Exception
	{
		super.tearDown();
		rmMountDir();

	}

	public void testInitialize()
	{
		assertEquals("VirtualFS is not set", virtualFs, fs.getVirtualFs());
		assertEquals("ControlNode not set", controlNode, fs.getControlNode());
		assertEquals("Homedirectory does not match working directory", fs
			.getHomeDirectory(), fs.getWorkingDirectory());
	}


	public void testGetUri() throws URISyntaxException
	{
		assertEquals("URI must match fs.default.name property", new URI(
			"psdfs://filesystem@10.200.2.10"), fs.getUri());

	}

	public void testGetHomeDirectory()
	{
		assertEquals(String.format("psdfs://%s@%s/user/hadoop", virtualFs,
					   controlNode), fs.getHomeDirectory().toString());
	}

	public void testGetWorkingDirectory()
	{
		assertEquals("", fs.getHomeDirectory(), fs.getWorkingDirectory());
	}

	public void testStartLocalOutput() throws IOException
	{
		final Path localTmpPath = new Path("/tmp/test_"
							   + System.currentTimeMillis());
		final Path startLocalOutput = fs.startLocalOutput(new Path("input/test"),
								  localTmpPath);
		assertEquals(localTmpPath, startLocalOutput);
	}

	public void testCompleteLocalOutput() throws IOException
	{
		final Path localTmpPath = new Path("/tmp/test_"
							   + System.currentTimeMillis());
		final Path psdfsPath = new Path("test");
		final Path startLocalOutput = fs.startLocalOutput(new Path("input/test"),
								  localTmpPath);
		assertEquals(localTmpPath, startLocalOutput);
		final File tmpFile = new File(startLocalOutput.toString());
		assertTrue(tmpFile.createNewFile());
		final String testTxt = "a Test Text";
		writeToFile(testTxt, tmpFile);

		fs.completeLocalOutput(psdfsPath, startLocalOutput);
		final File targetFile = fs.pathToFile(psdfsPath);

		assertFalse(tmpFile.exists());
		assertEquals(testTxt, readFile(targetFile));

	}

	public void testGetDefaultBlockSize() throws Exception
	{
		assertEquals(defaultBlockSize, fs.getDefaultBlockSize());
		assertNotSame(defaultBlockSize, 64);
		defaultBlockSize = 64;
		// reinit
		init();
		assertEquals(64, fs.getDefaultBlockSize());
	}

	public void testGetDefaultReplication() throws Exception
	{
		assertEquals(defaultReplication, fs.getDefaultReplication());
		assertNotSame(defaultReplication, 5);
		defaultReplication = 5;
		// reinit
		init();
		assertEquals(5, fs.getDefaultReplication());
	}

	public void testGetFileStatus() throws IOException
	{

		final FileStatus fileStatus = fs.getFileStatus(fs.getHomeDirectory());
		assertEquals(64 * 1024 * 1024, fileStatus.getBlockSize());

		fs.getConf().setLong(
			String.format(RawParascaleFileSystem.PS_VFS_BLOCKSIZE_FORMAT,
				      "myvirtualFs"), 64);
		try
		{

			fs.getFileStatus(new Path("psdfs://myvirtualFs@10.200.2.10"));
			fail("wrong fs");
		}
		catch (final IllegalArgumentException e)
		{
			// expected
		}

	}

	public void testPathToFile()
	{
		// test absolute
		assertEquals(new File(getTempDir(), String.format("%s/%s/%s/user/hadoop",
								  mountPoint, controlNode, virtualFs)), fs.pathToFile(fs
															      .getHomeDirectory()));
		// test relative

		assertEquals(new File(getTempDir(), String.format(
			"%s/%s/%s/user/hadoop/test", mountPoint, controlNode, virtualFs)), fs
			.pathToFile(new Path("test")));
	}

	public void testGetBlocksizeForPath()
	{

	}

	public void testGetVirtualFSFromPath()
	{
		assertEquals("myVirtualFs", fs.getVirtualFSFromPath(new Path(
			"psdfs://myVirtualFs@192.168.2.2/user/hadoop"), true));
		assertEquals("filesystem", fs.getVirtualFSFromPath(new Path("user/hadoop"),
								   false));
		assertEquals("filesystem", fs.getVirtualFSFromPath(
			new Path("/user/hadoop"), false));
		assertEquals("filesystem", fs.getVirtualFSFromPath(new Path("/"), false));
	}

	public void testRawParascaleFileSystem()
	{
		try
		{
			new RawParascaleFileSystem(null);
			fail("group info must not be null");
		}
		catch (final IllegalArgumentException e)
		{
			// expected
		}

		// happy path is tested during each setUp() call
	}

	public void testCopyFromLocal() throws Exception
	{
		// simple read write test -- the actual code is reused from hadoop
		// RawLocalFilesystem
		final File file = new File(tempDir, "copySrc");
		createRandomFile(RANDOM, file, 1024 * 1024);
		final String checksum = getMD5Checksum(file);
		final Path path = new Path("output");
		assertTrue(fs.mkdirs(path));
		final Path target = new Path(path, "target");
		fs.copyFromLocalFile(new Path(file.getAbsolutePath()), target);
		assertTrue("outputfile does not exist", new File(
			getLocalPathToWorkingDir(), "output/target").exists());
		assertTrue(fs.exists(target));
		assertEquals(checksum, getMD5Checksum(target, fs));
	}

	protected RawParascaleFileSystem getFileSystem(UserGroupInformation groupInformation)
	{
		return new RawParascaleFileSystemMock(groupInformation);
	}

	protected class RawParascaleFileSystemMock extends RawParascaleFileSystem
	{
		RawParascaleFileSystemMock(UserGroupInformation groupInformation)
		{
			super(groupInformation);
		}
	}
}
