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

public class ChunkLocatorTestHelper
{
	public static final String[] OBJ_INODE_FROM_FILE_COMMAND = ChunkLocator.OBJ_INODE_FROM_FILE_COMMAND;

	public static class Tuple<A, B>
	{
		A cur;
		B cud;

		public Tuple(A cur, B cud)
		{
			super();
			this.cur = cur;
			this.cud = cud;
		}

		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + ((cud == null) ? 0 : cud.hashCode());
			result = prime * result + ((cur == null) ? 0 : cur.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj)
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
			Tuple other = (Tuple) obj;
			if (cud == null)
			{
				if (other.cud != null)
				{
					return false;
				}
			}
			else if (!cud.equals(other.cud))
			{
				return false;
			}
			if (cur == null)
			{
				if (other.cur != null)
				{
					return false;
				}
			}
			else if (!cur.equals(other.cur))
			{
				return false;
			}
			return true;
		}

	}

}
