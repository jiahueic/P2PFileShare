package comp90015.idxsrv.peer;


import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingDeque;


import comp90015.idxsrv.filemgr.FileDescr;
import comp90015.idxsrv.filemgr.FileMgr;
import comp90015.idxsrv.message.*;
import comp90015.idxsrv.server.IOThread;
import comp90015.idxsrv.server.IndexElement;
import comp90015.idxsrv.textgui.ISharerGUI;
import comp90015.idxsrv.filemgr.FileDescr;
import org.json.JSONObject;

/**
 * Skeleton Peer class to be completed for Project 1.
 * @author aaron
 *
 */
public class Peer implements IPeer {

	private BufferedReader bufferedReader;
	private BufferedWriter bufferedWriter;
	private IOThread ioThread;
	
	private LinkedBlockingDeque<Socket> incomingConnections;
	
	private ISharerGUI tgui;
	
	private String basedir;
	
	private int timeout;
	
	private int port; // port number of peer

	private int idxServerPort;

	// creates a hash map which stores the upload threads running
	HashMap<String,Thread> uploadFileThreads;


	
	public Peer(int port, String basedir, int socketTimeout, ISharerGUI tgui, int idxServerPort) throws IOException {

		this.tgui=tgui;
		this.port=port;
		this.timeout=socketTimeout;
		this.basedir=new File(basedir).getCanonicalPath();
		this.idxServerPort = idxServerPort;
		this.incomingConnections = new LinkedBlockingDeque<Socket>();
		this.uploadFileThreads = new HashMap<String, Thread>();
		ioThread = new IOThread(port,incomingConnections,socketTimeout,tgui);
		// peer acting as server
		ioThread.start();

	}

	// connect to index server
	// where to get IdxServerPort
	public void connect(){
		try{
			InetAddress address = InetAddress.getByName("localhost");
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress(address, idxServerPort), timeout);
			// implement the socket timeout

			InputStream inputStream = socket.getInputStream();
			OutputStream outputStream = socket.getOutputStream();
			bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
			bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
		} catch (UnknownHostException e) {
			tgui.logWarn(e.getMessage());
		} catch (IOException e) {
			tgui.logWarn(e.getMessage());
		}


	}
	
	public void shutdown() throws InterruptedException, IOException {
		ioThread.shutdown();
		ioThread.interrupt();
		ioThread.join();
	}
	
	/*
	 * Students are to implement the interface below.
	 */
	
	@Override
	public void shareFileWithIdxServer(File file, InetAddress idxAddress, int idxPort, String idxSecret,
			String shareSecret) {
		// how to handle single request per connection protocol?
		try {
		connect();
		// send an authenticate request to the server
		// first, read the welcome message
		String welcomeMess = bufferedReader.readLine();
		Message welcome = (Message) MessageFactory.deserialize(welcomeMess);
		if(welcome.getClass().getName() == WelcomeMsg.class.getName()){
			WelcomeMsg welcomeMsg = (WelcomeMsg) welcome;
			tgui.logInfo(welcomeMsg.msg);
		}
		AuthenticateRequest authenticateRequest = new AuthenticateRequest(idxSecret);
		String authReqMessage = MessageFactory.serialize(authenticateRequest);
		bufferedWriter.write(authReqMessage);
		bufferedWriter.newLine();
		bufferedWriter.flush();
		String authReqRep = bufferedReader.readLine();
		AuthenticateReply authenticateReply = (AuthenticateReply) MessageFactory.deserialize(authReqRep);
		RandomAccessFile sharedFile = new RandomAccessFile(file, "r");
		FileDescr fd = new FileDescr(sharedFile);
		Message shareRequest = new ShareRequest(fd, file.getName(), shareSecret, port);
		// serialise the message
		String message = MessageFactory.serialize(shareRequest);
		bufferedWriter.write(message);
		bufferedWriter.newLine();
		bufferedWriter.flush();
		String status = "Sharing";
		// get the number of sharer
		// numSharers and status
		// create a shareRecord
		String replyMess = bufferedReader.readLine();
		ShareReply shareReply = (ShareReply) MessageFactory.deserialize(replyMess);
		long currNumSharers = (long) shareReply.numSharers;
		FileMgr fileMgr = new FileMgr(file.getPath(),fd);
		ShareRecord shareRecord = new ShareRecord(fileMgr,currNumSharers,status,idxAddress,idxPort,idxSecret,shareSecret);
		String relativePathName = file.getPath();
		tgui.addShareRecord(relativePathName, shareRecord);
		// need to open a peer serversocket here to send files to other people (threading)
		UploadClient uploadClient = new UploadClient(fileMgr,tgui,incomingConnections);
		Thread thread = new Thread(uploadClient);
		thread.start();
		uploadFileThreads.put(relativePathName,thread);

		}
		catch (FileNotFoundException e) {
			tgui.logError(e.getMessage());

		}
		catch (IOException e){
			tgui.logWarn(e.getMessage());

		}
		catch (NoSuchAlgorithmException e){
			tgui.logError(e.getMessage());
		} catch (JsonSerializationException e) {
			throw new RuntimeException(e);
		}


		// tgui.logError("shareFileWithIdxServer unimplemented");
	}

	@Override
	public void searchIdxServer(String[] keywords, 
			int maxhits, 
			InetAddress idxAddress, 
			int idxPort, 
			String idxSecret) {
		try{
			connect();
			// send an authenticate request to the server
			String welcomeMess = bufferedReader.readLine();
			Message welcome = (Message) MessageFactory.deserialize(welcomeMess);
			if(welcome.getClass().getName() == WelcomeMsg.class.getName()){
				WelcomeMsg welcomeMsg = (WelcomeMsg) welcome;
				tgui.logInfo(welcomeMsg.msg);
			}
			AuthenticateRequest authenticateRequest = new AuthenticateRequest(idxSecret);
			String authReqMessage = MessageFactory.serialize(authenticateRequest);
			bufferedWriter.write(authReqMessage);
			bufferedWriter.newLine();
			bufferedWriter.flush();
			String authReqRep = bufferedReader.readLine();
			AuthenticateReply authenticateReply = (AuthenticateReply) MessageFactory.deserialize(authReqRep);
			Message searchRequest = new SearchRequest(maxhits,keywords);
			String message = MessageFactory.serialize(searchRequest);
			bufferedWriter.write(message);
			bufferedWriter.newLine();
			bufferedWriter.flush();
			// tgui.logError("searchIdxServer unimplemented");
			// reads the reply of filenames from the server
			String filenamesMesg = bufferedReader.readLine();
			Object fileNames = MessageFactory.deserialize(filenamesMesg);
			SearchReply searchReply = (SearchReply) fileNames;
			// searchReply into SearchRecord
			for (int i = 0; i < searchReply.hits.length; i ++){
				FileDescr  filedescE = searchReply.hits[i].fileDescr;
				String fileMd5 = filedescE.getFileMd5();
				// indexServerSecret & numSharers
				// where to get filename
				String fileName = searchReply.hits[i].filename;
				Long currSharingPeers = new Long(searchReply.seedCounts[i]);
				InetAddress address = InetAddress.getByName(searchReply.hits[i].ip);
				SearchRecord searchRecord = new SearchRecord(filedescE,currSharingPeers,address,searchReply.hits[i].port,idxSecret,searchReply.hits[i].secret);
				tgui.addSearchHit(searchReply.hits[i].filename, searchRecord);
			}

		} catch(IOException e) {
			tgui.logWarn("IO error");
		}
		catch(JsonSerializationException e){
			tgui.logWarn("Request can't be serialised");
		}

	}

	@Override
	public boolean dropShareWithIdxServer(String relativePathname, ShareRecord shareRecord) {
		try{
			connect();
			// send an authenticate request to the server
			String welcomeMess = bufferedReader.readLine();
			Message welcome = (Message) MessageFactory.deserialize(welcomeMess);
			if(welcome.getClass().getName() == WelcomeMsg.class.getName()){
				WelcomeMsg welcomeMsg = (WelcomeMsg) welcome;
				tgui.logInfo(welcomeMsg.msg);
			}
			String idxSecret = shareRecord.idxSrvSecret;
			AuthenticateRequest authenticateRequest = new AuthenticateRequest(idxSecret);
			String authReqMessage = MessageFactory.serialize(authenticateRequest);
			bufferedWriter.write(authReqMessage);
			bufferedWriter.newLine();
			bufferedWriter.flush();
			String authReqRep = bufferedReader.readLine();
			AuthenticateReply authenticateReply = (AuthenticateReply) MessageFactory.deserialize(authReqRep);
			FileMgr filemanager = shareRecord.fileMgr;
			FileDescr fileDescr = filemanager.getFileDescr();
			String fileMd5 = fileDescr.getFileMd5();
			String sharerSecret = shareRecord.sharerSecret;
			// filename
			int lastSlash = relativePathname.lastIndexOf("/");
			String fileName = relativePathname.substring(lastSlash + 1);
			Message dropShareRequest = new DropShareRequest(fileName,fileMd5,sharerSecret,port);
			String message = MessageFactory.serialize(dropShareRequest);
			bufferedWriter.write(message);
			bufferedWriter.newLine();
			bufferedWriter.flush();
			String reply = bufferedReader.readLine();
			Message replyObj = (Message) MessageFactory.deserialize(reply);
			// access the hash with the relative pathname and delete the thread
			// remove deletes the mapping and returns the value (object) mapped to the key
			Thread uploadFileThread = uploadFileThreads.remove(relativePathname);
			uploadFileThread.interrupt();

		}
		catch (JsonSerializationException | IOException e){
			tgui.logWarn("Java serialisation or IO error");
		}
		//tgui.logError("dropShareWithIdxServer unimplemented");
		return false;
	}

	@Override
	public void downloadFromPeers(String relativePathname, SearchRecord searchRecord) {
		//search record to get fileMd5
		try{
			FileDescr fileDescr = searchRecord.fileDescr;
			String fileMd5 = fileDescr.getFileMd5();
			//int lastSlashIndex = relativePathname.lastIndexOf("/");
			//String fileName = relativePathname.substring(lastSlashIndex);
			String fileName = relativePathname;
			connect();
			String welcomeMess = bufferedReader.readLine();
			Message welcome = (Message) MessageFactory.deserialize(welcomeMess);
			if(welcome.getClass().getName() == WelcomeMsg.class.getName()){
				WelcomeMsg welcomeMsg = (WelcomeMsg) welcome;
				tgui.logInfo(welcomeMsg.msg);
			}
			// send an authenticate request to the server
			String idxSecret = searchRecord.idxSrvSecret;
			AuthenticateRequest authenticateRequest = new AuthenticateRequest(idxSecret);
			String authReqMessage = MessageFactory.serialize(authenticateRequest);
			bufferedWriter.write(authReqMessage);
			bufferedWriter.newLine();
			bufferedWriter.flush();
			String authReqRep = bufferedReader.readLine();
			AuthenticateReply authenticateReply = (AuthenticateReply) MessageFactory.deserialize(authReqRep);
			//lookup request
			LookupRequest lookupRequest = new LookupRequest(fileName,fileMd5);
			String lookupReqMess = MessageFactory.serialize(lookupRequest);
			// write this request to the server
			bufferedWriter.write(lookupReqMess);
			bufferedWriter.newLine();
			bufferedWriter.flush();
			String lookupReplyMess = bufferedReader.readLine();
			Object lookupReplyObj = MessageFactory.deserialize(lookupReplyMess);
			LookupReply lookupReply = (LookupReply) lookupReplyObj;
			IndexElement[] peerList = lookupReply.hits;

			// remote peer server will initialise their own ServerSocket
			DownloadClient downloadClient = new DownloadClient(fileName, peerList,tgui);
			Thread thread = new Thread(downloadClient);
			thread.start();

			//tgui.logError("downloadFromPeers unimplemented");
		}
		catch(JsonSerializationException e){
			tgui.logError("Java request serialisation failed");
		}
		catch(IOException e){
			tgui.logWarn("IO operation failed.");
		}

	}
	
}
