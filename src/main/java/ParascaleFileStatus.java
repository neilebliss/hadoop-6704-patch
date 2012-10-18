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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicBoolean;

class ParascaleFileStatus extends FileStatus
{

	private AtomicBoolean permissionLoaded = new AtomicBoolean(false);

	/**
	 * @param length           Length of the File
	 * @param isdir            true if the refered file if a directory
	 * @param blockReplication Number of copies of the file keept in the system
	 * @param blocksize        block size of the file
	 * @param modificationTime timestamp of last modificcation access
	 * @param path             Path to the file
	 */
	ParascaleFileStatus(final long length, final boolean isdir,
			    final int blockReplication, final long blocksize,
			    final long modificationTime, final Path path)
	{
		super(length, isdir, blockReplication, blocksize, modificationTime, path);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FsPermission getPermission()
	{
		loadPermissionInfo();
		return super.getPermission();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getOwner()
	{
		loadPermissionInfo();
		return super.getOwner();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getGroup()
	{
		loadPermissionInfo();
		return super.getGroup();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException
	{
		loadPermissionInfo();
		super.write(out);
	}

	static String execCommand(final File f, final String... cmd)
		throws IOException
	{
		final String[] args = new String[cmd.length + 1];
		System.arraycopy(cmd, 0, args, 0, cmd.length);
		args[cmd.length] = f.getCanonicalPath();
		final String output = Shell.execCommand(args);
		return output;
	}

	// / loads permissions, owner, and group from `ls -ld`
	void loadPermissionInfo()
	{

		if (permissionLoaded.get())
		{
			return;
		}

		IOException e = null;

		try
		{
			final StringTokenizer t = new StringTokenizer(getPermissionString());
			// expected format
			// -rw------- 1 username groupname ...

			String permission = t.nextToken();

			if (permission.length() > 10)
			{
				permission = permission.substring(0, 10);
			}

			setPermission(FsPermission.valueOf(permission));

			t.nextToken();

			setOwner(t.nextToken());
			setGroup(t.nextToken());

		}
		catch (final Shell.ExitCodeException ioe)
		{
			if (ioe.getExitCode() != 1)
			{
				e = ioe;
			}
			else
			{
				setPermission(null);
				setOwner(null);
				setGroup(null);
			}

		}
		catch (final IOException ioe)
		{
			e = ioe;

		}
		finally
		{
			if (e != null)
			{
				throw new RuntimeException("Error while running command to get "
								   + "file permissions : " + StringUtils.stringifyException(e));
			}
			permissionLoaded.set(true);
		}

	}

	protected String getPermissionString() throws IOException
	{
		// refactored for unit-test
		return execCommand(new File(getPath().toUri()), Shell
			.getGET_PERMISSION_COMMAND());
	}

	public String toString()
	{
		StringBuilder builder = new StringBuilder("FileStatus: ");
		builder.append("file: ").append(this.getPath()).append(" ");
		builder.append("length: ").append(this.getLen()).append(" ");

		return builder.toString();
	}

	@Override
	public boolean equals(Object o)
	{
		// We don't do anything special here, but we'll
		// explicitly mention it to make Hudson happy.
		return super.equals(o);
	}

	public int hashCode()
	{
		// We don't do anything special here, but we'll
		// explicitly mention it to make Hudson happy.
		return super.hashCode();
	}

	public AtomicBoolean getPermissionLoaded()
	{
		return permissionLoaded;
	}

}
