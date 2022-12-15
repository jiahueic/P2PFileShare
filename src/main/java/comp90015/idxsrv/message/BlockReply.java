package comp90015.idxsrv.message;
@JsonSerializable
public class BlockReply extends Message {
    @JsonElement
    private String filename;
    @JsonElement
    private String fileMd5;
    @JsonElement
    public Integer blockIdx;
    @JsonElement
    public String bytes;
    public BlockReply(){

    }
    public BlockReply(String filename, String fileMd5, Integer blockIdx, String bytes){
        this.filename = filename;
        this.fileMd5 = fileMd5;
        this.blockIdx = blockIdx;
        this.bytes = bytes;
    }
}
