package comp90015.idxsrv.peer;

import comp90015.idxsrv.filemgr.FileDescr;
import comp90015.idxsrv.filemgr.FileMgr;
import comp90015.idxsrv.message.*;
import comp90015.idxsrv.server.IndexElement;
import comp90015.idxsrv.textgui.ISharerGUI;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class DownloadClient implements Runnable {
    private Socket peerSocket;
    private String address;
    private int port;
    public BufferedWriter bufferedWriter;
    public BufferedReader bufferedReader;
    private boolean connected;
    private String filename;
    private FileDescr newFileDesc;
    private FileDescr oriFileDesc;
    private FileMgr fileMgr;
    private ISharerGUI tgui;

    private IndexElement[] peerList;
    public DownloadClient(String filename, IndexElement[] peerList, ISharerGUI tgui){
//        this.address = address;
//        this.port = port;
        this.peerList = peerList;
        this.filename = filename;
        this.tgui = tgui;
        try {
            // original file descriptor from the search record
            oriFileDesc = peerList[0].fileDescr;

            // File descriptor for local file location to download request to
            //newFileDesc = new FileDescr(new RandomAccessFile(filename,"rw"));
            // create a FileMgr to write to local file
            this.fileMgr = new FileMgr(filename, oriFileDesc);

            // try to connect to the peer
            connected = connect();
            InputStream inputStream = peerSocket.getInputStream();
            OutputStream outputStream = peerSocket.getOutputStream();
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
        } catch (IOException | NoSuchAlgorithmException e){
            tgui.logError("Error due to IO or no such algorithm");
        }
    }

    private boolean connect() throws IOException {
          for (int i=0; i<peerList.length; i++){
            peerSocket = new Socket(peerList[i].ip, peerList[i].port);
            if(peerSocket.isConnected() && !peerSocket.isClosed()){
                return true;
            }
        }
          return false;
    }

    public void run(){
        if (connected){
            // need to get the old file descriptor from the Search Record

            for (int blockId = 0; blockId<oriFileDesc.getNumBlocks(); blockId++){
                BlockRequest blockRequest = new BlockRequest(filename, oriFileDesc.getFileMd5(), blockId);

                try {
                    // request the block
                    String message = MessageFactory.serialize(blockRequest);
                    bufferedWriter.write(message);
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                    // process block and write to file
                    String response = bufferedReader.readLine();
                    BlockReply blockReply = (BlockReply) MessageFactory.deserialize(response);
                    byte[] blockByte = Base64.getDecoder().decode(blockReply.bytes);
                    fileMgr.writeBlock(blockReply.blockIdx,blockByte);
                } catch (JsonSerializationException | IOException e) {
                         e.printStackTrace();
                }
            }
                // send goodbye to server
                Goodbye goodbye = new Goodbye();
                try {
                    String goodByeMess = MessageFactory.serialize(goodbye);
                    bufferedWriter.write(goodByeMess);
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                } catch (IOException e) {
                    tgui.logError("IO error encountered when sending goodbye message.");
                }
                catch(JsonSerializationException e){
                     tgui.logError("Serialisation error encountered when sending goodbye");
                }

        }
    }
}
