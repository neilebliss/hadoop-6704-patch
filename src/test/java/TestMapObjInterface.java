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
import org.apache.commons.httpclient.HttpClient;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.ArrayList;

public class TestMapObjInterface extends TestCase
{

	private static final long CHUNK_SIZE = 67108864;

	public static ArrayList<String> getChunks(String file, String fsName,
						  String cnaddress) throws Exception
	{
		ArrayList<String> output = new ArrayList<String>();
		File target = new File(file);
		HttpClient client = new HttpClient();
		URI uri = new URI("http://" + cnaddress + ":14149/mproxy/map_obj");
		HTTPChunkLocator loc = new HTTPChunkLocator(client, uri);
		ChunkLocation[] chunks = loc.getChunkLocations(target, fsName);
		int i = 0;
		for (i = 0; i < chunks.length; i++)
		{
			StringBuilder b = new StringBuilder();
			b.append("[" + i + "] " + chunks[i].getChunkInfo().toString());
			b.append(": Filelength " + chunks[i].getFileLength());
			StorageNodeInfo[] nodes = chunks[i].getStorageNodeInfo();
			int j = 0;
			for (j = 0; j < nodes.length; j++)
			{
				b.append(" <" + j + ">" + nodes[j].toString());
			}
			output.add(b.toString());
		}
		return output;
	}

	public void testMapObjInterface()
	{
		assertEquals(0, 0);
		return;
	}

	public static boolean runTest(String file, String fsName, String cnaddress)
	{
		try
		{
			long targetNumChunks = 2;
			FileWriter godot = new FileWriter(file);
			File monet = new File(file);
			godot.write("I'm waiting.");
			godot.flush();
			while ((monet.length() / CHUNK_SIZE + 1) < targetNumChunks)
			{
				godot.write("I'm waiting.");
			}
			godot.flush();
			godot.close();
			ArrayList<String> chunks = getChunks(file, fsName, cnaddress);
			long fileChunks = (monet.length() / CHUNK_SIZE) + 1;
			if (fileChunks == chunks.size())
			{
				return true;
			}
			else
			{
				System.out.print("expected " + fileChunks + " chunks, got "
							 + chunks.size() + ": ");
				return false;
			}
		}
		catch (Exception e)
		{
			return false;
		}
	}

	public static void main(String[] args)
	{
		if (args.length < 3)
		{
			System.out
				.println("Usage: MapObjInterfaceTest <filename> <vfs name> <CN address>");
			System.out
				.println("MapObjInterfaceTest is a unit test of mproxy's map_obj interface."
						 + "  It expects as input a filename and a Parascale vfs name.");
			System.out
				.println("It returns a listing of the chunks for the specified file"
						 + " in the following format:");
			System.out.println();
			System.out
				.println("[1275] ChunkInfo: offset: 85563801600 length: 67108864 17 : "
						 + "Filelength 85899345920 <0>StorageNodeInfo: nodeName: "
						 + "atg-sn2 primar addr: 10.10.249.222 enabled: true up: true ");
			System.out.println();
			System.out
				.println("Callers may use this information to determine if the "
						 + "map_obj interface is returning correct data.");
			System.exit(1);
		}
		try
		{
			if (args[args.length - 1].equals("auto"))
			{
				if (runTest(args[0], args[1], args[2]))
				{
					System.out.println("pass");
				}
				else
				{
					System.out.println("FAIL");
				}
			}
			else
			{
				ArrayList<String> output = getChunks(args[0], args[1], args[2]);
				if (!output.isEmpty())
				{
					int i = 0;
					for (i = 0; i < output.size(); i++)
					{
						System.out.println(output.get(i));
					}
				}
			}
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
	}
}
