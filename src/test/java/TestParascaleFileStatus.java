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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Testcase for {@link ParascaleFileStatus}.
 */
public class TestParascaleFileStatus extends TestCase
{

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void setUp() throws Exception
	{
		super.setUp();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void tearDown() throws Exception
	{
		super.tearDown();
	}

	public void testParascaleFileStatus()
	{
		final Path p = new Path("/foo/bar");
		final long filesize = 10;
		final ParascaleFileStatusMock parascaleFileStatus = new ParascaleFileStatusMock(
			filesize, false, 2, 32 * 1024 * 1024, System.currentTimeMillis(), p);
		parascaleFileStatus.permissionString = "-rw-r-xr-- 1 parascale parascale 0 Sep  9 12:37 16:43 bar";

		assertEquals("invalid blocksize", 32 * 1024 * 1024, parascaleFileStatus
			.getBlockSize());
		assertEquals("invalid user", "parascale", parascaleFileStatus.getOwner());
		assertEquals("invalid group", "parascale", parascaleFileStatus.getGroup());
		assertEquals("invalid size of file - should be 10 as set in contructor",
			     10, parascaleFileStatus.getLen());
	}

	public void testLoadPermissionInfo()
	{
		final Path p = new Path("/foo/bar");
		{
			final ParascaleFileStatusMock parascaleFileStatus = new ParascaleFileStatusMock(
				10, false, 2, 32 * 1024 * 1024, System.currentTimeMillis(), p);
			parascaleFileStatus.permissionString = "-rw-r-xr-- 1 parascale parascale 0 Sep  9 12:37 16:43 bar";
			final FsPermission permission = parascaleFileStatus.getPermission();
			assertEquals(FsAction.READ, permission.getOtherAction());
			assertEquals(FsAction.READ_EXECUTE, permission.getGroupAction());
			assertEquals(FsAction.READ_WRITE, permission.getUserAction());
		}
		{
			final ParascaleFileStatusMock parascaleFileStatus = new ParascaleFileStatusMock(
				10, false, 2, 32 * 1024 * 1024, System.currentTimeMillis(), p);
			parascaleFileStatus.permissionString = "-rw--wxr-- 1 parascale parascale 0 Sep  9 12:37 16:43 bar";
			assertEquals(32 * 1024 * 1024, parascaleFileStatus.getBlockSize());
			assertEquals("parascale", parascaleFileStatus.getOwner());
			final FsPermission permission = parascaleFileStatus.getPermission();
			assertEquals(FsAction.READ, permission.getOtherAction());
			assertEquals(FsAction.WRITE_EXECUTE, permission.getGroupAction());
			assertEquals(FsAction.READ_WRITE, permission.getUserAction());

		}
		final ParascaleFileStatusMock parascaleFileStatus = new ParascaleFileStatusMock(
			10, false, 2, 32 * 1024 * 1024, System.currentTimeMillis(), p);
		parascaleFileStatus.permissionString = "-rw-r-xr-- 1 parascale parascale 0 Sep  9 12:37 16:43 bar";
		assertEquals("permissions already loaded - should be lazy", 0,
			     parascaleFileStatus.count.get());
		parascaleFileStatus.getPermission();
		assertEquals("permissions loaded more than once", 1,
			     parascaleFileStatus.count.get());
		parascaleFileStatus.getOwner();
		assertEquals("permissions loaded more than once", 1,
			     parascaleFileStatus.count.get());
		parascaleFileStatus.getGroup();
		assertEquals("permissions loaded more than once", 1,
			     parascaleFileStatus.count.get());
	}

	static class ParascaleFileStatusMock extends ParascaleFileStatus
	{
		public String permissionString;
		public AtomicInteger count = new AtomicInteger(0);

		ParascaleFileStatusMock(final long length, final boolean isdir,
					final int block_replication, final long blocksize,
					final long modification_time, final Path path)
		{
			super(length, isdir, block_replication, blocksize, modification_time,
			      path);
		}

		@Override
		protected String getPermissionString() throws IOException
		{
			count.incrementAndGet();
			return permissionString;
		}

	}

}
