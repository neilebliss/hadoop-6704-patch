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
 * A chunk is a part of a file starting at an offset. This class is the
 * representation of those chunks.
 */
public class ChunkInfo implements Comparable<ChunkInfo>
{
	private final long      chunkOffset;
	private final long      chunkLength;
	private final Integer[] volumeIds;

	ChunkInfo(final long chunkOffset, final long chunkLength,
		  final Integer[] volumeIds)
	{
		super();
		this.chunkOffset = chunkOffset;
		this.chunkLength = chunkLength;
		this.volumeIds = volumeIds;
	}

	/**
	 * Get the offset the chunk.
	 *
	 * @return offset of chunk
	 */
	public long getChunkOffset()
	{
		return chunkOffset;
	}

	/**
	 * Get length of the Chunk.
	 *
	 * @return length of chunk
	 */
	public long getChunkLength()
	{
		return chunkLength;
	}

	/**
	 * Get the id of the chunks volume.
	 *
	 * @return volume id
	 */
	public Integer[] getVolumeId()
	{
		return volumeIds;
	}

	@Override
	public int compareTo(final ChunkInfo o)
	{
		Long thisOffset = Long.valueOf(chunkOffset);
		Long anotherOffset = Long.valueOf(o.chunkOffset);
		return thisOffset.compareTo(anotherOffset);
	}

	public String toString()
	{
		StringBuilder builder = new StringBuilder("ChunkInfo: ");
		builder.append("offset: ").append(chunkOffset).append(" ");
		builder.append("length: ").append(chunkLength).append(" ");
		for (Integer id : volumeIds)
		{
			builder.append(id).append(" ");
		}
		return builder.toString();

	}

	public boolean equals(Object o)
	{
		if (o == null)
		{
			return false;
		}
		if (this == o)
		{
			return true;
		}
		if (!(o instanceof ChunkInfo))
		{
			return false;
		}
		ChunkInfo other = (ChunkInfo) o;
		if (this.getChunkOffset() == other.getChunkOffset())
		{
			if (this.getChunkLength() == other.getChunkLength())
			{
				return true;
			}
		}
		return false;
	}

	public int hashCode()
	{
		Long myHashValue = Long.valueOf(chunkOffset * Long.valueOf(chunkLength));
		return myHashValue.hashCode();
	}


}
