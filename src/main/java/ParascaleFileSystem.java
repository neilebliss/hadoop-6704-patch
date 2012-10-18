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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

/**
 * {@link ParascaleFileSystem} is an implementation for reading and writing
 * files stored on an NFS mounted <a href="http://www.parascale.com/">Parascale
 * Distributed Filesystem</a>
 * <p>
 * This class provides raw access to the underlying NFS filesystem as well as an
 * optional filtered checksum support (see {@link ChecksumFileSystem}). The NFS
 * filesystem tries to mount a filesystem for a given path using the authority
 * part of the paths URI. A path like psdfs://parascale@10.200.1.12 specifies a
 * virtual-filesystem with the name <i>parascale</i> on host <i>10.200.1.12</i>.
 * <br>
 * On a filesystem level this path translates to
 * <i>/mountpoint/10.200.1.12/parascale</i> where the <i>mountpoint</i> is
 * configured using the Hadoop Configuration property
 * {@link RawParascaleFileSystem#PS_MOUNT_POINT}.
 * </p>
 * <p>
 * For detailed documentation see {@link RawParascaleFileSystem}.
 * </p>
 *
 * @see RawParascaleFileSystem
 * @see ParascaleFileStatus
 */
public class ParascaleFileSystem extends FilterFileSystem
{

	/**
	 * The Hadoop Property that enables / disables a {@link ChecksumFileSystem}
	 * used by the parascale filesystem. If the property
	 * <i>parascale.fs.usecrc</i> is set to <code>true</code> the
	 * {@link ParascaleFileSystem} will use a {@link ChecksumFileSystem} wrapper
	 * otherwise no checksum will be calculated for files written to the
	 * filesystem.
	 */
	public static final String CRC_FILESYSTEM = "parascale.fs.usecrc";

	/**
	 * Create a new {@link ParascaleFileSystem} instance. Hadoop Requires a public
	 * default constructor to load filesystem implementations via the reflection
	 * API
	 */
	public ParascaleFileSystem()
	{
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(final URI uri, final Configuration conf)
		throws IOException
	{
		final URI rawUri;
		final RawParascaleFileSystem rawParascaleFileSystem;
		UserGroupInformation groupInformation;
		try
		{
			if (conf.get("hadoop.job.ugi") != null)
			{
				String username = new StringTokenizer(conf.get("hadoop.job.ugi"), ",")
					.nextToken();
				groupInformation = UserGroupInformation.createRemoteUser(username);
			}
			else
			{
				groupInformation = UserGroupInformation.getCurrentUser();
			}
			rawParascaleFileSystem = new RawParascaleFileSystem(groupInformation);
			fs = conf.getBoolean(CRC_FILESYSTEM, false) ? new ChecksumFsWrapper(
				rawParascaleFileSystem) : rawParascaleFileSystem;
			rawUri = new URI(uri.getScheme(), uri.getAuthority(), null, null, null);
		}
		catch (final URISyntaxException e)
		{
			throw (IOException) new IOException().initCause(e);
		}
		// initialize with the raw URI - RawFS expects it without a path!
		fs.initialize(rawUri, conf);
		if (!rawParascaleFileSystem.isMountPointAbsolute())
		{
			throw new IOException("Mountpoint "
						      + rawParascaleFileSystem.getMountPoint() +
						      " is not an absolute path");
		}
		if (!rawParascaleFileSystem.mountPointExists())
		{
			throw new IOException(
				"WorkingDirectory does not exist - can not mount Parascale " +
					"filesystem at " + rawParascaleFileSystem.getMountPath());
		}
		if (!rawParascaleFileSystem.createHomeDirectory())
		{
			throw new IOException("Can not create HomeDirectory");
		}

	}

	/**
	 * A {@link ChecksumFileSystem} with a default implemenation.
	 */
	private static class ChecksumFsWrapper extends ChecksumFileSystem
	{

		public ChecksumFsWrapper(final FileSystem fs)
		{
			super(fs);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean reportChecksumFailure(final Path f,
						     final FSDataInputStream in, final long inPos,
						     final FSDataInputStream sums, final long sumsPos)
		{
			return super.reportChecksumFailure(f, in, inPos, sums, sumsPos);
		}

	}

}
