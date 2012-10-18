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
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * The HTTPChunkLocator class implements the REST API access for determining
 * chunk locations.
 */

public class HTTPChunkLocator extends ChunkLocator
{

	private static final String CHUNK_ID     = "chunk";
	private static final String COPY_ID      = "copy";
	private static final QName  Q_IP_ADDR    = new QName("ip_addr");
	private static final QName  Q_NAME       = new QName("name");
	private static final QName  Q_VID        = new QName("vid");
	private static final QName  Q_CHUNK_SIZE = new QName("chunk_size");
	private static final QName  Q_OFFSET     = new QName("offset");

	@Override
	public void release() throws ChunkStorageException
	{

	}

	@Override
	public ChunkLocation[] getChunkLocations(final File file,
						 final String virtualFsName) throws ChunkStorageException
	{
		final long inode = getInode(file);
		final long fileSizeInBytes = getFileSizeInBytes(file);

		final XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLEventReader reader = null;
		try
		{
			final URI uri = new URI(endpoint.getScheme(), endpoint.getUserInfo(),
						endpoint.getHost(), endpoint.getPort(), endpoint.getPath(), "inum="
				+ inode, null);
			reader = factory.createXMLEventReader(openStream(uri));
			final List<ChunkLocation> parseChunks = parseChunks(reader,
									    fileSizeInBytes);
			return parseChunks.toArray(new ChunkLocation[0]);
		}
		catch (final Exception e)
		{
			throw new ChunkStorageException(e);
		}
		finally
		{
			if (reader != null)
			{
				try
				{
					reader.close();
				}
				catch (final XMLStreamException e)
				{
					// ignore
				}
			}
		}
	}

	private final HttpClient client;
	private final URI        endpoint;
	public static final String DEFAULT_PATH = "mproxy/map_obj";

	public HTTPChunkLocator(final HttpClient client, URI aUri)
	{
		final HttpConnectionManagerParams params = client
			.getHttpConnectionManager().getParams();
		params.setConnectionTimeout(10000);
		params.setTcpNoDelay(true);
		params.setDefaultMaxConnectionsPerHost(10);
		client.getHttpConnectionManager().setParams(params);
		this.client = client;
		this.endpoint = aUri;

	}

	private InputStream openStream(final URI aUri) throws IOException
	{
		final GetMethod method = new GetMethod(aUri.toString());
		method.setFollowRedirects(true);
		client.executeMethod(method);
		final int statusCode = method.getStatusCode();
		if (statusCode != 200)
		{
			method.abort();
			throw new IOException(String.format("HTTP Status Code %s for URL %s",
							    statusCode, aUri));
		}
		return method.getResponseBodyAsStream();
	}

	/**
	 * parses a chunk info part
	 * <p/>
	 * <pre>
	 *  <chunk_list>
	 *    <chunk offset="0" chunk_size="67108864">
	 *      <map>
	 *        <copy ip_addr="10.10.2.200" name="vistasn1" snid="1" vid="1" />
	 *        <copy ip_addr="10.10.2.201" name="vistasn2" snid="2" vid="3" />
	 *     </map>
	 *    </chunk>
	 *  </chunk_list>
	 * </pre>
	 */
	private List<ChunkLocation> parseChunks(final XMLEventReader reader,
						final long fileSizeInBytes) throws XMLStreamException, IOException
	{
		long offset = -1;
		long length = -1;
		final List<ChunkLocation> locations = new ArrayList<ChunkLocation>(5);
		final List<StorageNodeInfo> sninfo = new ArrayList<StorageNodeInfo>();
		final List<Integer> volumeIds = new ArrayList<Integer>();
		while (reader.hasNext())
		{
			final XMLEvent nextEvent = reader.nextEvent();
			if (!(nextEvent instanceof StartElement))
			{
				continue;
			}
			final StartElement start = (StartElement) nextEvent;
			if (CHUNK_ID.equals(start.getName().getLocalPart()))
			{
				if (sninfo.size() > 0)
				{
					final ChunkInfo chunkInfo = new ChunkInfo(offset, length, volumeIds
						.toArray(new Integer[0]));
					locations.add(new ChunkLocation(sninfo
										.toArray(new StorageNodeInfo[0]), chunkInfo, fileSizeInBytes));
					sninfo.clear();
					volumeIds.clear();
				}
				offset = Long.parseLong(get(start, Q_OFFSET));
				length = Long.parseLong(get(start, Q_CHUNK_SIZE));

			}
			else if (COPY_ID.equals(start.getName().getLocalPart()))
			{
				final InetAddress ip = InetAddress.getByName(get(start, Q_IP_ADDR));
				final int vid = Integer.parseInt(get(start, Q_VID));
				final String name = get(start, Q_NAME);
				sninfo.add(new StorageNodeInfo(name, ip.getHostAddress(), true, true));
				volumeIds.add(Integer.valueOf(vid));

			}
		}
		if (sninfo.size() > 0)
		{
			final ChunkInfo chunkInfo = new ChunkInfo(offset, length, volumeIds
				.toArray(new Integer[0]));
			locations.add(new ChunkLocation(sninfo.toArray(new StorageNodeInfo[0]),
							chunkInfo, fileSizeInBytes));
		}
		return locations;
	}

	private static String get(final StartElement element, final QName name)
		throws IOException
	{
		final Attribute attributeByName = element.getAttributeByName(name);
		if (attributeByName == null)
		{
			throw new IOException("missing attribute " + name.toString());
		}
		return attributeByName.getValue();
	}

	@Override
	public String toString()
	{
		return "HTTPChunkLocator [endpoint=" + endpoint + "]";
	}

}
