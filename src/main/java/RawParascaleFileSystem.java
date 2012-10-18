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

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link RawParascaleFileSystem} This class implements the Parascale
 * filesystem. It defines the main properties of the filesystem and grants
 * access to the basic filesystem and parascale specific operations and
 * informations.
 * <p>
 * Parascale has a notation of a virtual filesystem. Each virtual filesystem can
 * be mounted separately and may have different configuration values, space
 * limitations and permissions. In the case of hadoop a file or a directory in a
 * filesystem is represented as a {@link Path}. Such a path has a URI
 * representation unique for each {@link FileSystem} implementation identified
 * by the <i>scheme</i> part of the URI. {@link RawParascaleFileSystem} and its
 * public wrapper class {@link ParascaleFileSystem} use the scheme
 * <i>psdfs://</i> followed by an authority part. The authority part encodes the
 * virtual filesystem and the host / IP this filesystem can be accessed through
 * separated by the <code>@</code> character.
 * </p>
 * <p>
 * The <i>path</i> part of the {@link URI} reference a file on this virtual
 * filesystem. Nevertheless the path gets translated into a per user
 * subdirectory of the filesystem. For each command executed on the filesystem
 * like {@link FileSystem#create(Path)} or {@link FileSystem#mkdirs(Path)} the
 * username of the user executing the command is used as a prefix plus an
 * additional <code>/user</code> toplevel directory.
 * </p>
 * <p>
 * Parascale exports filesystem via three different interfaces HTTP, FTP and
 * NFS. This implementation uses a locally available NFS mount to access the a
 * certain virtual filesystem. To locate the mount point of a virtual filesystem
 * {@link RawParascaleFileSystem} accesses a hadoop configuration property (
 * {@link RawParascaleFileSystem#PS_MOUNT_POINT}) to build the path to the
 * filesystem mount. If this property is not set the default mount point
 * (<i>/net</i>) is used instead.
 * </p>
 * <p>
 * For example the URI:
 * <code>psdfs://storageVFS@10.200.1.2/input/test.txt</code> <br>
 * Translates to /net/10.200.1.2/user/root/input/test.txt if the create command
 * is executed as the user <i>root</i>
 * </p>
 * <p>
 * Parascale does not expose any API to set replication factors and file
 * blocksize on a per file basis. Those values are configured per virtual
 * filesystem while hadoop has no direct access to it. Yet, especially the
 * blocksize is important for hadoop to calculate the split size for instance
 * during a Map/Reduce job. To enable hadoop to access those values the
 * blocksize and the replication factor can be set as a hadoop configuration
 * property on a per virtual filesystem basis. For example to set the
 * replication factor to <i>3</i> and the blocksize to 64MB for the virtual
 * filesystem <i>storageVFS</i> set the following properties to the
 * <i>hadoop-site.xml</i> file:
 * <p/>
 * <pre>
 *  &lt;property&gt;
 *    &lt;name&gt;parascale.fs.storageVFS.blocksize&lt;/name&gt;
 *    &lt;value&gt;64&lt;/value&gt;
 *  &lt;/property&gt;
 *  &lt;property&gt;
 *    &lt;name&gt;parascale.fs.storageVFS.replication&lt;/name&gt;
 *    &lt;value&gt;3&lt;/value&gt;
 *  &lt;/property&gt;
 * </pre>
 * <p/>
 * </p>
 *
 * @see ParascaleFileStatus
 * @see ParascaleFileSystem
 */
class RawParascaleFileSystem extends RawLocalFileSystem
{

	/*
	   * Name definitions of properties used in configuration to obtain their values
	   */
	/**
	 * The version information for this instance of psdfs.
	 */
	static final String PSDFS_VERSION_STRING =
		" ParaScale FS Hadoop Integration - version 0.2 ";

	/**
	 * The Hadoop Property to set the mountpoint where parascale filesystem can be
	 * mounted on a local storage medium <i>parascale.fs.mountpoint</i>. This
	 * defaults to <i>/net</i>
	 */
	static final String PS_MOUNT_POINT = "parascale.fs.mountpoint";

	/**
	 * The Hadoop Property to set the default replication factor for parascale
	 * fileystems. <i>parascale.fs.default.replication</i>
	 */
	static final String PS_DEFAULT_REPLICATION =
		"parascale.fs.default.replication";

	/**
	 * The Hadoop Property to set the default blocksize for parascale filesystems.
	 * <i>parascale.fs.default.blocksize</i>
	 */
	static final String PS_DEFAULT_BLOCKSIZE = "parascale.fs.default.blocksize";

	/**
	 * The Hadoop Property to set a blocksize for a certain virtual filesystem.
	 * <i>parascale.fs.%s.blocksize</i> where the %s should be replaced with the
	 * virtual filesystem name. If no value can be found the default blocksize is
	 * used.
	 */
	static final String PS_VFS_BLOCKSIZE_FORMAT = "parascale.fs.%s.blocksize";

	/**
	 * The Hadoop Property to set a replication factor for a certain virtual
	 * filesystem. <i>parascale.fs.%s.replication</i> where the %s should be
	 * replaced with the virtual filesystem name. If no value can be found the
	 * default replication factor is used.
	 */
	static final String PS_VFS_REPLICATION_FORMAT = "parascale.fs.%s.replication";

	/**
	 * The Hadoop Property to set the host name of the control node.
	 */
	static final String CHUNKS_CONTROLNODE = "parascale.fs.chunks.controlnode";

	/**
	 * The Hadoop Property to set the chunk REST endpoint. The endpoint must be a
	 * URI
	 */
	static final String CHUNKS_REST_ENDPOINT =
		"parascale.fs.chunks.rest.endpoint";

	/**
	 * Default virtual filesystem name. This value is set to
	 * <code>filesystem</code>
	 */
	public static final String DEFAULT_FILESYSTEM = "filesystem";

	/**
	 * Default local mount point. This value is set to <code>/net</code>.
	 */
	public static final  String DEFAULT_MOUTPOINT   = "/net";
	/*
	   * default replication factor for PS
	   */
	private static final short  DEFAULT_REPLICATION = 2;

	/*
	   * Default blocksize for PS in MB
	   */
	private static final short DEFAULT_BLOCKSIZE = 64;

	/*
	   * Other static strings - uncategorized
	   */
	private static final String FS_NAME   = "parascale";
	private static final String TO_STRING = "Parascale Hadoop - FileSystem";

	private String virtualFs;

	private       String               controlNode;
	private       URI                  uri;
	private final UserGroupInformation groupInformation;
	private       Path                 workingDirectory;

	private Log pLog = LogFactory.getLog(RawParascaleFileSystem.class);

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getDefaultBlockSize()
	{
		// Parascale has 64MB blocksize by default
		return getConf().getLong(PS_DEFAULT_BLOCKSIZE,
					 DEFAULT_BLOCKSIZE * 1024 * 1024);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public short getDefaultReplication()
	{
		return (short) getConf().getInt(PS_DEFAULT_REPLICATION, 2);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileStatus getFileStatus(final Path f) throws IOException
	{
		final File pathToFile = pathToFile(f);
		if (!pathToFile.exists())
		{
			throw new FileNotFoundException(String
								.format("File %s does not exist", f));
		}
		return new ParascaleFileStatus(pathToFile.length(), pathToFile
			.isDirectory(), getReplicationForPath(f), getBlocksizeForPath(f),
					       pathToFile.lastModified(), f);
	}

	/**
	 * Get the blocksize for a given Path <code>aPath</code>.
	 *
	 * @param aPath Virtual filesystem path to get the blocksize from
	 *
	 * @return Blocksize of the virtual filesystem
	 */
	long getBlocksizeForPath(final Path aPath)
	{
		long blocksize = getDefaultBlockSize();
		final String vfsName = getVirtualFSFromPath(aPath, false);
		if (vfsName != null)
		{
			blocksize = getConf().getLong(
				String.format(PS_VFS_BLOCKSIZE_FORMAT, vfsName), DEFAULT_BLOCKSIZE)
				* 1024 * 1024;
		}
		return blocksize;
	}

	/**
	 * Amount of replications a given path <code>aPath</code> should have.
	 *
	 * @param aPath Path to the File
	 *
	 * @return number of replications in the filesystem
	 */
	short getReplicationForPath(final Path aPath)
	{
		short replication = getDefaultReplication();
		final String vfsName = getVirtualFSFromPath(aPath, false);
		if (vfsName != null)
		{
			replication = (short) getConf().getInt(
				String.format(PS_VFS_REPLICATION_FORMAT, vfsName),
				DEFAULT_REPLICATION);
		}
		return replication;
	}

	/**
	 * Get the virtual filesystem of a path <code>aPath</code>. The VFS is
	 * determined by the qualified uri of the path. If the path is not qualified
	 * it will made qualified.
	 *
	 * @param aPath       Path to virtual filesystem
	 * @param isQualified Must be set if the path is not qualified
	 *
	 * @return virtual filesystem
	 */
	String getVirtualFSFromPath(final Path aPath, final boolean isQualified)
	{
		final Path makeQualified = isQualified ? aPath :
			aPath.makeQualified(this.getUri(), this.getWorkingDirectory());
		return parseAuthority(makeAbsolute(makeQualified).toUri())[0];
	}

	/**
	 * Parsing the an uri and extract the virtualFS and host name from the
	 * authority part of the {@link URI}. If no authority is set the default
	 * authority of the filesystem will be returned
	 *
	 * @param aUri uri to extract the authority from
	 *
	 * @return An array of strings where the first element is the Parascale
	 *         virtualFS and the second is the host of the NFS mount
	 */
	static String[] parseAuthority(final URI aUri)
	{
		final String authority = aUri.getAuthority();
		final int indexOf = authority.indexOf('@');
		// if @ is not present assume default
		if (indexOf == -1)
		{
			return new String[]{
				DEFAULT_FILESYSTEM, authority
			};
		}
		// virtualFs, controlNode
		return new String[]{
			authority.substring(0, indexOf),
			authority.substring(indexOf + 1, authority.length())
		};
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BlockLocation[] getFileBlockLocations(final FileStatus file,
						     final long start, final long len) throws IOException
	{
		ChunkLocator newChunkLocator = null;
		if (file.getLen() < start + len)
		{
			throw new IOException("start+len must be less or equal than file length");
		}
		final ArrayList<BlockLocation> locations = new ArrayList<BlockLocation>();
		try
		{
			newChunkLocator = newChunkLocator();
			final Path makeQualified = file.getPath().
				makeQualified(this.getUri(), this.getWorkingDirectory());

			// sorted by offset
			final ChunkLocation[] chunkLocations = newChunkLocator.getChunkLocations(
				pathToFile(makeQualified), getVirtualFSFromPath(makeQualified, true));
			long begin = start;
			long length = len;
			for (final ChunkLocation chunkLocation : chunkLocations)
			{
				final ChunkInfo chunkInfo = chunkLocation.getChunkInfo();
				final StorageNodeInfo[] storageNodeInfo = chunkLocation
					.getStorageNodeInfo();
				if (length <= 0)
				{
					// stop when length exceeded
					break;
				}
				if (begin < chunkInfo.getChunkOffset())
				{
					// skip if location not reached yet
					continue;
				}
				final List<String> hosts = new ArrayList<String>(0);
				for (int j = 0; j < storageNodeInfo.length; j++)
				{
					// select all enabled and running nodes
					if (storageNodeInfo[j].isUp() && storageNodeInfo[j].isEnabled())
					{
						hosts.add(storageNodeInfo[j].getNodeName());
					}
				}
				final long lengthInChunk = chunkInfo.getChunkLength()
					- (begin - chunkInfo.getChunkOffset());
				final BlockLocation blockLocation = new BlockLocation(null, hosts
					.toArray(new String[0]), begin,
										      lengthInChunk < length ? lengthInChunk : length);
				begin += blockLocation.getLength();
				length -= blockLocation.getLength();
				locations.add(blockLocation);

			}
			if (pLog.isDebugEnabled())
			{
				pLog.debug("Fetched " + locations.size() + " chunk locations for "
						   + makeQualified);
			}

			return locations.toArray(new BlockLocation[0]);

		}
		catch (final ChunkStorageException e)
		{
			throw new IOException(
				"can not fetch chunk locations " + newChunkLocator == null ? ""
					: newChunkLocator.toString(), e);
		}
		finally
		{
			if (newChunkLocator != null)
			{
				newChunkLocator.close();
			}
		}
	}

	RawParascaleFileSystem(final UserGroupInformation aGroupInformation)
	{
		super();
		if (aGroupInformation == null)
		{
			throw new IllegalArgumentException("aGroupInformation must not be null");
		}
		groupInformation = aGroupInformation;

	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws IOException
	 */
	@Override
	public void initialize(final URI aUri, final Configuration aConfiguration)
		throws IOException
	{
		if (pLog.isInfoEnabled())
		{
			pLog.info(PSDFS_VERSION_STRING);
		}
		super.initialize(aUri, aConfiguration);
		final String[] parseAuthority = parseAuthority(aUri);
		final String controlNodeName = parseAuthority[1];
		String virtualFsName = parseAuthority[0];
		if (controlNodeName == null)
		{
			// URI must contain the control node IP or name as the host part,
			// but was null
			throw new IllegalArgumentException(
				"null host section for control node URI.");
		}
		if (virtualFsName == null)
		{
			virtualFsName = DEFAULT_FILESYSTEM;
		}
		controlNode = controlNodeName;
		virtualFs = virtualFsName;
		uri = aUri;
		workingDirectory = getHomeDirectory();
		if (pLog.isDebugEnabled())
		{
			pLog.debug("Successfully initialized filesystem - working dir:"
					   + workingDirectory);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Path getHomeDirectory()
	{
		return new Path(getRawHomeDirectory()).
			makeQualified(this.getUri(), this.getWorkingDirectory());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Path getWorkingDirectory()
	{
		return workingDirectory;
	}

	/**
	 * Returns the local path to the parascale environment home path.
	 *
	 * @return Path to the home path
	 */
	private String getRawHomeDirectory()
	{

		return String.format("/user/%s", groupInformation.getUserName());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public File pathToFile(final Path path)
	{
		// this has public modifier due to subclassing
		checkPath(path);
		final String mountPoint = getMountPoint();
		final File file;

		if (path.isAbsolute())
		{
			final URI pathUri = path.
				makeQualified(this.getUri(), this.getWorkingDirectory()).toUri();
			final String[] parseAuthority = parseAuthority(pathUri);

			file = new File(mountPoint, String.format("/%s/%s/%s", parseAuthority[1],
								  parseAuthority[0], pathUri.getPath()));

		}
		else
		{
			file = new File(mountPoint, String.format("/%s/%s/%s/%s", controlNode,
								  virtualFs, getRawHomeDirectory(), path.toUri().getPath()));
		}
		return file;
	}

	/**
	 * Converts a relative path to its absolute representation. If the path
	 * already is an absolute path it will not be converted.
	 *
	 * @param aPath relative path
	 *
	 * @return absolute path
	 */
	Path makeAbsolute(final Path aPath)
	{
		if (aPath.isAbsolute())
		{
			return aPath;
		}
		return new Path(workingDirectory, aPath);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void completeLocalOutput(final Path fsWorkingFile,
					final Path tmpLocalFile) throws IOException
	{
		// move from local when local write is done
		moveFromLocalFile(tmpLocalFile, fsWorkingFile);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Path startLocalOutput(final Path fsOutputFile, final Path tmpLocalFile)
		throws IOException
	{
		// return the local file and move it to the mounted fs on completion
		return tmpLocalFile;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void copyFromLocalFile(final boolean delSrc, final boolean overwrite,
				      final Path[] srcs, final Path dst) throws IOException
	{
		final Configuration conf = getConf();
		FileUtil.copy(getLocal(conf), srcs, this, dst, delSrc, overwrite, conf);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void moveFromLocalFile(final Path src, final Path dst)
		throws IOException
	{
		copyFromLocalFile(true, src, dst);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public URI getUri()
	{
		return uri == null ? super.getUri() : uri;
	}

	@Override
	public String toString()
	{
		return TO_STRING;
	}

	/**
	 * Get the name of the Virtual filesystem.
	 *
	 * @return the name of the parascale virtual filesystem
	 */
	String getVirtualFs()
	{
		return virtualFs;
	}

	/**
	 * URI for the parascale filesystem control node.
	 *
	 * @return uri of controlnode
	 */
	String getControlNode()
	{
		return controlNode;
	}

	/**
	 * Creates the home directory of the parascale environment.
	 *
	 * @return true if the home directory exists.
	 *
	 * @throws IOException
	 */
	boolean createHomeDirectory() throws IOException
	{
		if (!exists(getHomeDirectory()))
		{

			final File file = new File(String.format("%s/user/%s", getMountPath(),
								 groupInformation.getUserName()));
			return file.mkdirs();
		}
		return true;
	}

	/**
	 * Test whether the moint point of the parascale system exists or not.
	 *
	 * @return true if the mount point exists.
	 */
	boolean mountPointExists()
	{
		final String mountPoint = getMountPoint();
		return new File(mountPoint, String.format("/%s/%s", controlNode, virtualFs))
			.exists();
	}

	/**
	 * The path of the parascale mount point.
	 *
	 * @return Path of the parascale mount point
	 */
	String getMountPoint()
	{
		final Configuration conf = getConf();
		return conf.get(PS_MOUNT_POINT, DEFAULT_MOUTPOINT);
	}

	/**
	 * Test whether the mount point is an absolute path starting with '/'.
	 *
	 * @return <code>true</code> if the mount point is absolute. Otherwise
	 *         <code>false</code>
	 */
	boolean isMountPointAbsolute()
	{
		return getMountPoint().startsWith("/");
	}

	/**
	 * URI to to the mount path.
	 *
	 * @return mount path URI
	 */
	String getMountPath()
	{
		return String.format("%s/%s/%s", getMountPoint(), controlNode, virtualFs);
	}

	/**
	 * Creates a new {@link ChunkLocator} instance.
	 *
	 * @return a new {@link ChunkLocator} instance
	 *
	 * @throws ChunkStorageException if the {@link ChunkLocator} throws an
	 *                               {@link ChunkStorageException}
	 */
	protected ChunkLocator newChunkLocator() throws ChunkStorageException
	{
		final Configuration conf = getConf();
		final URI endpoint = URI.create(conf.get(CHUNKS_REST_ENDPOINT,
							 "http://localhost:14149/" + HTTPChunkLocator.DEFAULT_PATH));
		if (pLog.isDebugEnabled())
		{
			pLog
				.debug("Create new REST based chunk locator - endpoint IP: "
					       + endpoint);
		}
		return new HTTPChunkLocator(HttpClientHolder.client, endpoint);
	}

	private static class HttpClientHolder
	{
		static final HttpClient client = new HttpClient();
	}

}
