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

/**
 * A chunk location keeps information about the overlying storage nodes.
 */
public class ChunkLocation
{

	private final long              fileLength;
	private final StorageNodeInfo[] storageNodeInfo;
	private final ChunkInfo         chunkInfo;

	ChunkLocation(final StorageNodeInfo[] aStorageNodeInfo,
		      final ChunkInfo aChunkInfo, final long fileLength)
	{
		super();
		storageNodeInfo = aStorageNodeInfo;
		chunkInfo = aChunkInfo;
		this.fileLength = fileLength;
	}

	/**
	 * Get information of the storage nodes containing the chunks.
	 *
	 * @return storage node information
	 */
	public StorageNodeInfo[] getStorageNodeInfo()
	{
		return storageNodeInfo;
	}

	/**
	 * Get the length of the File.
	 *
	 * @return length of file
	 */
	public long getFileLength()
	{
		return fileLength;
	}

	/**
	 * Get chunk informations.
	 *
	 * @return chunk information
	 */
	public ChunkInfo getChunkInfo()
	{
		return chunkInfo;
	}

	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		builder.append(chunkInfo).append("\n");
		for (StorageNodeInfo nodeinfo : storageNodeInfo)
		{
			builder.append(nodeinfo).append("\n");
		}
		builder.append("filelength: ").append(fileLength);
		return builder.toString();

	}

}
