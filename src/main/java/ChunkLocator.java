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

import org.apache.hadoop.util.Shell;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * This class implements the base functions need to determine all chunks of a
 * path within the virtual file system.
 */
public abstract class ChunkLocator
{

	static final String[] OBJ_INODE_FROM_FILE_COMMAND = new String[]{
		"ls",
		"-i1"
	}; // inode

	public abstract ChunkLocation[] getChunkLocations(File aFile,
							  String aVirtualFsName) throws ChunkStorageException;

	/**
	 * Release the chunk locater.
	 *
	 * @throws ChunkStorageException
	 */
	public abstract void release() throws ChunkStorageException;

	/**
	 * Close the chunk locate. This method implicitly calls release().
	 */
	public void close()
	{
		try
		{
			release();
		}
		catch (final Exception e)
		{
			// close quiet
		}
	}

	/**
	 * Returns the inode of the given file
	 *
	 * @param aFile the file to resolve the inode for
	 *
	 * @return the inode of the given file
	 *
	 * @throws ChunkStorageException if the command execution throws an exception
	 */
	public long getInode(File aFile) throws ChunkStorageException
	{
		try
		{
			final String output = execCommand(aFile, OBJ_INODE_FROM_FILE_COMMAND);
			StringTokenizer outputFields = new StringTokenizer(output.trim());
			String inodeNumber = outputFields.nextToken();
			return Long.parseLong(inodeNumber.trim());
		}
		catch (final Exception e)
		{
			throw new ChunkStorageException(
				"Inode determination command execution failed", e);
		}
	}

	/**
	 * Returns the size in bytes of the given file
	 *
	 * @param aFile the file to resolve the size for
	 *
	 * @return the size of the given file
	 *
	 * @throws ChunkStorageException if the File class call throws an exception
	 */
	public long getFileSizeInBytes(File aFile) throws ChunkStorageException
	{
		try
		{
			return aFile.length();
		}
		catch (final Exception e)
		{
			throw new ChunkStorageException("File size determination failed", e);
		}

	}

	protected String execCommand(File aFile, String[] command)
		throws IOException
	{
		final String[] args = new String[command.length + 1];
		System.arraycopy(command, 0, args, 0, command.length);
		args[command.length] = aFile.getCanonicalPath();
		return Shell.execCommand(args);
	}

}
