package LogParser.DRAPA;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import provenenceGraph.dataModel.PDM;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static LogParser.NodLink.win10LogParser.*;
import static LogParser.NodLink.win10LogParser.convertToIP;
import static provenenceGraph.dataModel.PDM.File.FileType.FILE_UNKNOWN;
import static provenenceGraph.dataModel.PDM.LogContent.*;
import static provenenceGraph.dataModel.PDM.LogContent.PROCESS_EXEC;

public class TheiaLogParser {
    public Long eventTimeStamp;
    private PDM.Log.Builder log_builder;
    private PDM.LogHeader uheader;
    public Map<UUID, PDM.NetEvent> netFlowObjectCache;   // PDM has no Event , so use NetEvent, whose direction can not be identified with single object
    public Map<UUID, PDM.File> fileObjectCache;
    public Map<UUID, PDM.Process> processObjectCache;

    public Map<String, PDM.LogContent> logContentTable;
    public UUID subjectUUID;
    public UUID predicateObjectUUID;
    public TheiaLogParser() {
        this.netFlowObjectCache = new HashMap<>();
        this.fileObjectCache = new HashMap<>();
        this.processObjectCache = new HashMap<>();
        this.logContentTable = new HashMap<>();
        logContentTable.put("EVENT_OPEN", FILE_OPEN);
        logContentTable.put("EVENT_READ", FILE_READ);
        logContentTable.put("EVENT_WRITE", FILE_WRITE);
        logContentTable.put("EVENT_CONNECT", NET_CONNECT);
        logContentTable.put("EVENT_RECVFROM", NET_CONNECT);
        logContentTable.put("EVENT_SENDTO", NET_CONNECT);
        logContentTable.put("EVENT_CLONE", PROCESS_FORK);
        logContentTable.put("EVENT_EXECUTE", PROCESS_EXEC);
    }

    /**
     *  OPEN WRITE READ
     */
    public PDM.Log InitLog(Integer process_id, String process_name, String filepath, long filePathHash){

        PDM.EventHeader eventHeader = setEventHeader(process_id, 0, process_name, eventTimeStamp);
        PDM.FileEvent fileEvent = setFileEvent(FILE_UNKNOWN, filePathHash, filepath);

        return this.log_builder.setUHeader(uheader).setEventData(
                PDM.EventData.newBuilder()
                        .setEHeader(eventHeader)
                        .setFileEvent(fileEvent)
        ).build();
    }
    /**
     *  SEND  RECV  CONNECT
     */
    public PDM.Log InitLog(Integer process_id, String subjectProcessName, UUID uuid, PDM.NetEvent.Direction direction){

        PDM.EventHeader eventHeader = setEventHeader(process_id, 0, subjectProcessName, eventTimeStamp);

        PDM.NetEvent netEvent = setNetEvent(this.netFlowObjectCache.get(uuid).getSip(), this.netFlowObjectCache.get(uuid).getDip(), this.netFlowObjectCache.get(uuid).getSport(), this.netFlowObjectCache.get(uuid).getDport(), direction);
        return this.log_builder.setUHeader(uheader).setEventData(PDM.EventData.newBuilder()
                                .setEHeader(eventHeader)
                                .setNetEvent(netEvent)
                ).build();
    }

    /**
     *  CLONE AND EXECUTE
     */
    public PDM.Log InitLog(Integer subjectProcessPID, String subjectProcessName, Integer objectProcessPID, String objectProcessName, String cmdLine){

        PDM.EventHeader eventHeader = setEventHeader(subjectProcessPID, 0, subjectProcessName, this.eventTimeStamp);
        PDM.ProcessEvent processEvent = setProcessEvent(objectProcessPID,0, objectProcessName, cmdLine);

        return this.log_builder.setUHeader(uheader).setEventData(
                        PDM.EventData.newBuilder()
                                .setEHeader(eventHeader)
                                .setProcessEvent(processEvent)
                )
                .build();
    }
    public PDM.Log jsonParse(String JsonData){
        //deSerialize
        JSONObject jsonObj;
        try {
            jsonObj = JSON.parseObject(JsonData);
        }catch (Exception e){
            return null;
        }
        this.log_builder = PDM.Log.newBuilder();
        //build LogHeader
        PDM.LogHeader.Builder uHeader_builder = PDM.LogHeader.newBuilder();
        uHeader_builder.setType(PDM.LogType.EVENT);

        Iterator<String> iterator = jsonObj.getJSONObject("datum").keySet().iterator();
        String logTypeComplete = iterator.next();
        String logType = logTypeComplete.substring(logTypeComplete.lastIndexOf('.') + 1);  // com.bbn.tc.schema.avro.cdm18.???

        if(logType.equals("Host") || logType.equals("Principle")){
            // special logs return
            return null;
        }
        else if(!logType.equals("Event")){
            JSONObject eventContent = jsonObj.getJSONObject("datum").getJSONObject("com.bbn.tc.schema.avro.cdm18." + logType);
            switch(logType){
                case "Subject":
                    String cmdLine = eventContent.getJSONObject("cmdLine")==null ? "unknown" : eventContent.getJSONObject("cmdLine").getString("string");
                    String processName = eventContent.getJSONObject("properties").getJSONObject("map").getString("path");
                    int ppid = eventContent.getJSONObject("properties").getJSONObject("map").containsKey("ppid")?eventContent.getJSONObject("properties").getJSONObject("map").getInteger("ppid"): 0 ;
                    Long timeStamp = eventContent.getLong("startTimestampNanos");

                    PDM.Process p = PDM.Process.newBuilder().setProcUUID(PDM.ProcessUUID.newBuilder().setPid(ppid).setTs(timeStamp)).setProcessName(processName).setCmdline(cmdLine).build();
                    this.processObjectCache.put(UUID.fromString(eventContent.getString("uuid")), p);
                    break;
                case "FileObject":
                    String filepath = eventContent.getJSONObject("baseObject").getJSONObject("properties").getJSONObject("map").getString("filename");
                    long filePathHash = UUID.nameUUIDFromBytes(filepath.getBytes(StandardCharsets.UTF_8)).getLeastSignificantBits();
                    PDM.File f = PDM.File.newBuilder().setFileType(FILE_UNKNOWN).setFileUUID(PDM.FileUUID.newBuilder().setFilePathHash(filePathHash).build()).setFilePath(filepath).build();
                    this.fileObjectCache.put(UUID.fromString(eventContent.getString("uuid")), f);
                    break;
                case "NetFlowObject":
                    String sourceIP = eventContent.getString("localAddress");
                    String destinationIP = eventContent.getString("remoteAddress");
                    if(sourceIP.equals("LOCAL")) sourceIP="localhost";
                    if(sourceIP.equals("NETLINK")) sourceIP="-1.-1.-1.-1";
                    if(destinationIP.equals("NETLINK")) destinationIP="-1.-1.-1.-1";
                    PDM.NetEvent n = setNetEvent(convertToIP(sourceIP), convertToIP(destinationIP), eventContent.getInteger("localPort"), eventContent.getInteger("remotePort"), PDM.NetEvent.Direction.NONE);
                    this.netFlowObjectCache.put(UUID.fromString(eventContent.getString("uuid")), n);
                    break;
            }
        }
        else{
            JSONObject eventContent = jsonObj.getJSONObject("datum").getJSONObject("com.bbn.tc.schema.avro.cdm18." + logType);
            this.eventTimeStamp = eventContent.getLong("timestampNanos");
            long hostUUID= UUID.fromString(jsonObj.getJSONObject("datum").getJSONObject("com.bbn.tc.schema.avro.cdm18." + logType).getString("hostId")).getLeastSignificantBits();
            this.uheader = uHeader_builder.setContent(this.logContentTable.get(eventContent.getString("type"))).setClientID(PDM.HostUUID.newBuilder().setHostUUID(hostUUID).build()).build();

            switch (eventContent.getString("type")){
                case "EVENT_OPEN":
                case "EVENT_READ":
                case "EVENT_WRITE":
                    this.subjectUUID = UUID.fromString(eventContent.getJSONObject("subject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    this.predicateObjectUUID = UUID.fromString(eventContent.getJSONObject("predicateObject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    return InitLog( this.processObjectCache.get(this.subjectUUID).getProcUUID().getPid(), this.processObjectCache.get(this.subjectUUID).getProcessName(), this.fileObjectCache.get(this.predicateObjectUUID).getFilePath(), this.fileObjectCache.get(this.predicateObjectUUID).getFileUUID().getFilePathHash());
                case "EVENT_CLONE":
                case "EVENT_EXECUTE":
                    this.subjectUUID = UUID.fromString(eventContent.getJSONObject("subject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    this.predicateObjectUUID = UUID.fromString(eventContent.getJSONObject("predicateObject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    return InitLog(this.processObjectCache.get(this.subjectUUID).getProcUUID().getPid(), this.processObjectCache.get(this.subjectUUID).getProcessName(), this.processObjectCache.get(this.predicateObjectUUID).getProcUUID().getPid(),this.processObjectCache.get(this.predicateObjectUUID).getProcessName(), this.processObjectCache.get(this.predicateObjectUUID).getCmdline());
                case "EVENT_CONNECT":
                case "EVENT_SENDTO":
                    this.subjectUUID = UUID.fromString(eventContent.getJSONObject("subject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    this.predicateObjectUUID = UUID.fromString(eventContent.getJSONObject("predicateObject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    return InitLog(this.processObjectCache.get(this.subjectUUID).getProcUUID().getPid(), this.processObjectCache.get(this.subjectUUID).getProcessName(), this.predicateObjectUUID, PDM.NetEvent.Direction.OUT);
                case "EVENT_RECVFROM":
                    this.subjectUUID = UUID.fromString(eventContent.getJSONObject("subject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    this.predicateObjectUUID = UUID.fromString(eventContent.getJSONObject("predicateObject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    return InitLog(this.processObjectCache.get(this.subjectUUID).getProcUUID().getPid(), this.processObjectCache.get(this.subjectUUID).getProcessName(), this.predicateObjectUUID, PDM.NetEvent.Direction.IN);
                default:
                    return null;
            }
        }
        return null;
    }
}
