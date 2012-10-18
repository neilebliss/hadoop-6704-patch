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
 * This class keeps information of a storage node within the parascale
 * filesystem. So it a representation of a physical storage node.
 */
public class StorageNodeInfo
{
	private final String  nodeName;
	private final String  nodePrimaryAddress;
	private final boolean isEnabled;
	private final boolean isUp;

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + (isEnabled ? 1231 : 1237);
		result = prime * result + (isUp ? 1231 : 1237);
		result = prime * result + (nodeName == null ? 0 : nodeName.hashCode());
		result = prime * result
			+ (nodePrimaryAddress == null ? 0 : nodePrimaryAddress.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return false;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		final StorageNodeInfo other = (StorageNodeInfo) obj;
		if (isEnabled != other.isEnabled)
		{
			return false;
		}
		if (isUp != other.isUp)
		{
			return false;
		}
		if (nodeName == null)
		{
			if (other.nodeName != null)
			{
				return false;
			}
		}
		else if (!nodeName.equals(other.nodeName))
		{
			return false;
		}
		if (nodePrimaryAddress == null)
		{
			if (other.nodePrimaryAddress != null)
			{
				return false;
			}
		}
		else if (!nodePrimaryAddress.equals(other.nodePrimaryAddress))
		{
			return false;
		}
		return true;
	}

	StorageNodeInfo(final String nodeName, final String nodePrimaryAddress,
			final boolean isEnabled, final boolean isUp)
	{
		super();
		this.nodeName = nodeName;
		this.nodePrimaryAddress = nodePrimaryAddress;
		this.isEnabled = isEnabled;
		this.isUp = isUp;
	}

	/**
	 * Get name of the Node.
	 *
	 * @return name of the node
	 */
	public String getNodeName()
	{
		return nodeName;
	}

	/**
	 * Get primary address of the node.
	 *
	 * @return primary address of the node
	 */
	public String getNodePrimaryAddress()
	{
		return nodePrimaryAddress;
	}

	/**
	 * Determine if the node is enabled or disabled.
	 *
	 * @return <code>True</code> if the node is enabled. <code>False</code>
	 *         otherwise.
	 */
	public boolean isEnabled()
	{
		return isEnabled;
	}

	/**
	 * Determine if the node has been started.
	 *
	 * @return <code>True</code> if the Node has been started. Returns
	 *         <code>false</code> otherwise.
	 */
	public boolean isUp()
	{
		return isUp;
	}

	public String toString()
	{
		StringBuilder builder = new StringBuilder("StorageNodeInfo: ");
		builder.append("nodeName: ").append(nodeName).append(" ");
		builder.append("primar addr: ").append(nodePrimaryAddress).append(" ");
		builder.append("enabled: ").append(isEnabled).append(" ");
		builder.append("up: ").append(isUp).append(" ");
		return builder.toString();

	}
}
