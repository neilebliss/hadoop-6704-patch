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

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.CharBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 * Base class for testcases related to the Parascale Hadoop Filesystem
 * implementation. This testcase emulates a NFS mounted parascale filesystem
 * under <code>/tmp/net/10.200.2.10/filesystem</code>
 * <p>
 * All necessary configuration values for the {@link ParascaleFileSystem} are
 * set during the testcases setup phase and can be modified by subclasses of
 * this testcase.
 * </p>
 */
public class ParascaleFsTestCase extends TestCase
{
	protected static final Random  RANDOM                = new Random();
	protected              long    defaultBlockSize      = 32;
	protected              boolean setDefaultBlockSize   = true;
	protected              long    defaultReplication    = 2;
	protected              boolean setDefaultReplication = true;
	protected              String  fsScheme              = "psdfs://";
	protected              boolean setDefaultFsName      = true;
	protected              String  controlNode           = "10.200.2.10";
	protected              String  virtualFs             = "filesystem";
	protected              String  mountPoint            = "net";
	protected              boolean setMountPoint         = true;
	static final           String  FS_DEFAULT_NAME       = "fs.default.name";
	protected final        File    tempDir               = new File(getTempDir(), "test");

	static final long currentTimeMillis = System.currentTimeMillis();

	static
	{
		// create a Random object to generate random test input
		RANDOM.setSeed(currentTimeMillis);
		System.out.println("RandomSeed: " + currentTimeMillis);
	}

	@Override
	protected void setUp() throws Exception
	{
		super.setUp();
		if ("windows".equals(System.getProperty("os.name").toLowerCase()))
		{
			fail("This testcase always fails on a windows platform - use UNIX/Linux");
		}
		tempDir.delete();
		if (!tempDir.exists())
		{
			assertTrue(tempDir.mkdirs());
		}
		rmMountDir();
		assertTrue("Failed to create " + getMountDir(), mkMountDir());
	}

	@Override
	protected void tearDown() throws Exception
	{
		super.tearDown();
		cleanTempDirs();
		tempDir.delete();
	}

	/**
	 * Creates a new Hadoop Configuration object.
	 *
	 * @return a new Hadoop configuration object
	 *
	 * @see Configuration
	 */
	protected Configuration getConf()
	{
		final Configuration conf = new Configuration();
		if (setDefaultBlockSize)
		{
			conf.setLong(RawParascaleFileSystem.PS_DEFAULT_BLOCKSIZE,
				     defaultBlockSize);
		}
		if (setDefaultReplication)
		{
			conf.setLong(RawParascaleFileSystem.PS_DEFAULT_REPLICATION,
				     defaultReplication);
		}
		if (setMountPoint)
		{
			conf.set(RawParascaleFileSystem.PS_MOUNT_POINT, String.format("%s/%s",
										      getTempDir(), mountPoint));
		}
		if (setDefaultFsName)
		{
			conf.set(FS_DEFAULT_NAME, String.format("%s%s@%s", fsScheme, virtualFs,
								controlNode));
		}

		return conf;
	}

	/**
	 * Returns the path to the temp dir to use in testing.
	 *
	 * @return the path to the Temp directory.
	 */
	protected static String getTempDir()
	{
		Long randSeed = Long.valueOf(currentTimeMillis);
		String mountpt = System.getProperty("java.io.tmpdir");
		String dir = mountpt + "/" + randSeed.toString() + "/";
		return dir;
	}

	/**
	 * Clean up any directories created during testing.
	 */

	protected static void cleanTempDirs()
	{
		File mnt = new File(getTempDir());
		recursiveFileDelete(mnt);
	}

	/**
	 * Traverse the given path, deleting all files and directories.
	 * This is the java equivalent of rm -rf.
	 */

	protected static boolean recursiveFileDelete(File path)
	{
		if (path.exists())
		{
			File[] files = path.listFiles();
			for (int i = 0; i < files.length; i++)
			{
				if (files[i].isDirectory())
				{
					recursiveFileDelete(files[i]);
				}
				else
				{
					files[i].delete();
				}
			}
		}
		return path.delete();
	}

	/**
	 * Creates the emulated mount directory for the {@link ParascaleFileSystem}
	 *
	 * @return <code>true</code> if the creation was successful otherwise
	 *         <code>false</code>
	 */
	protected boolean mkMountDir()
	{
		final File file = getMountDir();
		return file.mkdirs();
	}

	/**
	 * Removes the emulated mount directory for the {@link ParascaleFileSystem}
	 *
	 * @return <code>true</code> if the operation was successful otherwise
	 *         <code>false</code>
	 *
	 * @throws IOException
	 */
	protected boolean rmMountDir() throws IOException
	{

		return FileUtil.fullyDelete(getMountDir());
	}

	protected File getMountDir()
	{
		final File file = new File(getTempDir(), String.format("%s/%s/%s",
								       mountPoint, controlNode, virtualFs));
		return file;
	}

	/**
	 * Writes a given string into the given file. The file must already exist.
	 *
	 * @param aText a text to write
	 * @param aFile a file to write the text to
	 *
	 * @throws IOException if the write operation throws a {@link IOException}
	 */
	protected void writeToFile(final String aText, final File aFile)
		throws IOException
	{
		BufferedWriter writer = null;
		try
		{
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
				aFile)));
			writer.write(aText);
		}
		finally
		{
			if (writer != null)
			{
				writer.close();
			}
		}
	}

	/**
	 * Reads a given file into a String.
	 *
	 * @param aFile a file to read
	 *
	 * @return a string representation of the given file content
	 *
	 * @throws IOException if the read operation throws an {@link IOException}
	 */
	protected String readFile(final File aFile) throws IOException
	{
		BufferedReader reader = null;
		final CharBuffer buffer = CharBuffer.allocate(1024);
		final StringBuilder builder = new StringBuilder();
		try
		{
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(
				aFile)));
			int read = 0;
			while ((read = reader.read(buffer)) > 0)
			{
				builder.append(buffer.array(), 0, read);
				buffer.clear();
			}
		}
		finally
		{
			if (reader != null)
			{
				reader.close();
			}
		}
		return builder.toString();
	}

	/**
	 * Calculates the MD5 sum of a given file
	 *
	 * @param aFile the file to checksum
	 *
	 * @return the byte representation of the MD5 checksum
	 *
	 * @throws IOException              if an {@link IOException} is thrown during the caculation
	 * @throws NoSuchAlgorithmException if the MD5 algorithm is not present on the executing system.
	 */
	public static byte[] md5sum(final File aFile) throws IOException,
							     NoSuchAlgorithmException
	{
		return md5sum(new FileInputStream(aFile));
	}

	public static byte[] md5sum(final InputStream aStream) throws IOException,
								      NoSuchAlgorithmException
	{
		final InputStream inputStream = aStream;
		final byte[] buffer = new byte[1024];
		final MessageDigest complete = MessageDigest.getInstance("MD5");
		int numRead;
		try
		{
			while ((numRead = inputStream.read(buffer)) != -1)
			{
				if (numRead > 0)
				{
					complete.update(buffer, 0, numRead);
				}
			}
		}
		finally
		{
			if (inputStream != null)
			{
				inputStream.close();
			}
		}

		return complete.digest();
	}

	/**
	 * @param aFile
	 *
	 * @return
	 *
	 * @throws Exception
	 */
	public static String getMD5Checksum(final File aFile) throws Exception
	{
		final byte[] b = md5sum(aFile);
		return byteToHex(b);
	}

	public static String getMD5Checksum(final Path aPath,
					    final FileSystem aFileSystem) throws Exception
	{
		final byte[] b = md5sum(aFileSystem.open(aPath));
		return byteToHex(b);
	}

	private static String byteToHex(final byte[] b)
	{
		final StringBuilder result = new StringBuilder();
		for (int i = 0; i < b.length; i++)
		{
			result.append(Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1));
		}
		return result.toString();
	}

	protected static void createRandomFile(final Random random, final File aFile,
					       final int bytesToWrite) throws IOException
	{
		if (!aFile.exists())
		{
			assertTrue(aFile.createNewFile());
		}
		createRandomFile(random, new BufferedOutputStream(new FileOutputStream(
			aFile)), bytesToWrite);
	}

	protected static void createRandomFile(final Random random,
					       final OutputStream aStream, final int bytesToWrite) throws IOException
	{
		int bytesWritten = 0;
		final byte[] buffer = new byte[bytesToWrite < 1024 ? bytesToWrite : 1024];
		try
		{
			while (bytesWritten < bytesToWrite)
			{
				random.nextBytes(buffer);
				aStream.write(buffer);
				bytesWritten += buffer.length;
			}
		}
		finally
		{
			aStream.close();
		}
	}

	protected File getLocalPathToWorkingDir()
	{
		return new File(String.format("/%s/%s/%s/%s/user/hadoop", getTempDir(),
					      mountPoint, controlNode, virtualFs));
	}

}
