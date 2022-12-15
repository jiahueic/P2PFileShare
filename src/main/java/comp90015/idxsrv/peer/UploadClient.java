package comp90015.idxsrv.peer;

import comp90015.idxsrv.filemgr.BlockUnavailableException;
import comp90015.idxsrv.filemgr.FileMgr;
import comp90015.idxsrv.message.*;
import comp90015.idxsrv.textgui.ITerminalLogger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import java.util.RandomAccess;
import java.util.concurrent.LinkedBlockingDeque;

public class UploadClient extends Thread{
    BufferedWriter bufferedWriter;
    BufferedReader bufferedReader;

    LinkedBlockingDeque<Socket>incomingConnections;
    ITerminalLogger logger;

    FileMgr fileMgr;
    public UploadClient( FileMgr fileMgr, ITerminalLogger logger,  LinkedBlockingDeque<Socket>incomingConnections){
            this.fileMgr = fileMgr;
            this.incomingConnections= incomingConnections;
            this.logger = logger;
    }
    public void run() {
        logger.logInfo("Upload thread running.");
        // if thread is not interrupted
        while(! isInterrupted()) {
            try{
                Socket peerClient = incomingConnections.take();
                processRequest(peerClient);
                peerClient.close();
            }
            catch (InterruptedException e){
                logger.logWarn("The peer has stopped sharing the file.");
            }
            catch(IOException e) {
                logger.logWarn(e.getMessage());
            }
        }

    }

    public void processRequest(Socket peerClient) {
        try{
            InputStream inputStream = peerClient.getInputStream();
            OutputStream outputStream = peerClient.getOutputStream();
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
            // receive messages until goodbye
            while (true){
                String peerMess = bufferedReader.readLine();
                Message peerRequest = (Message) MessageFactory.deserialize(peerMess);
                if(peerRequest.getClass().getName()== BlockRequest.class.getName()){
                    BlockRequest blockRequest = (BlockRequest) peerRequest;
                    // need to find bytes of file
                    byte[] byteArray = fileMgr.readBlock(blockRequest.blockIdx);
                    String byteString = Base64.getEncoder().encodeToString(byteArray);
                    BlockReply blockReply = new BlockReply(blockRequest.filename,blockRequest.fileMd5,blockRequest.blockIdx,byteString);
                    String blockMess = MessageFactory.serialize(blockReply);
                    bufferedWriter.write(blockMess);
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                }
                else if(peerRequest.getClass().getName() == Goodbye.class.getName()){
                    // close the streams
                    bufferedReader.close();
                    bufferedWriter.close();
                    break;
                }
            }
        }
        catch(BlockUnavailableException e){
            e.printStackTrace();
        }
        catch(JsonSerializationException e){
            e.printStackTrace();
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }
}
