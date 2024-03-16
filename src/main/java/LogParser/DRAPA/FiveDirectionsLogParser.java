package LogParser.DRAPA;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import provenenceGraph.dataModel.PDM;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static LogParser.NodLink.win10LogParser.*;
import static LogParser.NodLink.win10LogParser.convertToIP;
import static provenenceGraph.dataModel.PDM.File.FileType.FILE_UNKNOWN;
import static provenenceGraph.dataModel.PDM.LogContent.*;
import static provenenceGraph.dataModel.PDM.LogContent.PROCESS_EXEC;

public class FiveDirectionsLogParser {
    public Long eventTimeStamp;
    private PDM.Log.Builder log_builder;
    private PDM.LogHeader uheader;
    public Map<UUID, PDM.NetEvent> netFlowObjectCache;   // PDM has no Event , so use NetEvent, whose direction can not be identified with single object
    public Map<UUID, PDM.File> fileObjectCache;
    public Map<UUID, PDM.Process> processObjectCache;

    public Map<String, PDM.LogContent> logContentTable;
    public UUID subjectUUID;
    public UUID predicateObjectUUID;
    public FiveDirectionsLogParser() {
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
        logContentTable.put("EVENT_FORK", PROCESS_FORK);
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

        if(logType.equals("Host") || logType.equals("Principle") || logType.equals("StartMarker") || logType.equals("TimeMarker") || logType.equals("EndMarker")){
            // special logs return
            return null;
        }
        else if(!logType.equals("Event")){
            JSONObject eventContent = jsonObj.getJSONObject("datum").getJSONObject("com.bbn.tc.schema.avro.cdm18." + logType);
            switch(logType){
                case "Subject":
                    String processName;
                    if(eventContent.getJSONObject("cmdLine")!=null)
                        processName = eventContent.getJSONObject("cmdLine").getString("string"); // use cmd as processName(refer to readme)
                    else {
                        if(eventContent.getJSONObject("parentSubject") == null)
                            processName = "unknown";
                        else
                            processName = this.processObjectCache.get(UUID.fromString(eventContent.getJSONObject("parentSubject").getString("com.bbn.tc.schema.avro.cdm18.UUID"))).getProcessName();
                    }
                    int ppid = eventContent.getInteger("cid");
                    Long timeStamp = eventContent.getLong("startTimestampNanos");
                    PDM.Process p = PDM.Process.newBuilder().setProcUUID(PDM.ProcessUUID.newBuilder().setPid(ppid).setTs(timeStamp)).setProcessName(processName).setCmdline("null").build();
                    this.processObjectCache.put(UUID.fromString(eventContent.getString("uuid")), p);
                    break;
                case "FileObject":  // filepath info is a part of event's info, record here for classify event type
                    String filepath = "null";
                    long filePathHash = UUID.nameUUIDFromBytes(filepath.getBytes(StandardCharsets.UTF_8)).getLeastSignificantBits();
                    PDM.File f = PDM.File.newBuilder().setFileType(FILE_UNKNOWN).setFileUUID(PDM.FileUUID.newBuilder().setFilePathHash(filePathHash).build()).setFilePath(filepath).build();
                    this.fileObjectCache.put(UUID.fromString(eventContent.getString("uuid")), f);
                    break;
                case "RegistryKeyObject":
                    String filepathReg = eventContent.getString("key");
                    long filePathHashReg = UUID.nameUUIDFromBytes(filepathReg.getBytes(StandardCharsets.UTF_8)).getLeastSignificantBits();
                    PDM.File fR = PDM.File.newBuilder().setFileType(FILE_UNKNOWN).setFileUUID(PDM.FileUUID.newBuilder().setFilePathHash(filePathHashReg).build()).setFilePath(filepathReg).build();
                    this.fileObjectCache.put(UUID.fromString(eventContent.getString("uuid")), fR);
                    break;
                case "NetFlowObject":
                    String sourceIP = eventContent.getString("localAddress").isEmpty()?"-1.-1.-1.-1":eventContent.getString("localAddress");
                    String destinationIP = eventContent.getString("remoteAddress").isEmpty()?"-1.-1.-1.-1":eventContent.getString("remoteAddress");
                    if(sourceIP.contains(":") || sourceIP.equals("NA")) sourceIP = "-1.-1.-1.-1";
                    if(destinationIP.contains(":") || destinationIP.equals("NA")) destinationIP = "-1.-1.-1.-1";
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
                case "EVENT_READ":
                case "EVENT_WRITE":
                case "EVENT_EXECUTE":
                    this.subjectUUID = UUID.fromString(eventContent.getJSONObject("subject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    String filepath = eventContent.getJSONObject("predicateObjectPath").getString("string");
                    return InitLog( this.processObjectCache.get(this.subjectUUID).getProcUUID().getPid(), this.processObjectCache.get(this.subjectUUID).getProcessName(), filepath, UUID.nameUUIDFromBytes(filepath.getBytes(StandardCharsets.UTF_8)).getLeastSignificantBits());
                case "EVENT_OPEN":
                    if(this.fileObjectCache.containsKey(UUID.fromString(eventContent.getJSONObject("predicateObject").getString("com.bbn.tc.schema.avro.cdm18.UUID")))){
                        this.subjectUUID = UUID.fromString(eventContent.getJSONObject("subject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                        String filepathOpen = eventContent.getJSONObject("predicateObjectPath").getString("string");
                        return InitLog( this.processObjectCache.get(this.subjectUUID).getProcUUID().getPid(), this.processObjectCache.get(this.subjectUUID).getProcessName(), filepathOpen, UUID.nameUUIDFromBytes(filepathOpen.getBytes(StandardCharsets.UTF_8)).getLeastSignificantBits());
                    }else if(this.processObjectCache.containsKey(UUID.fromString(eventContent.getJSONObject("predicateObject").getString("com.bbn.tc.schema.avro.cdm18.UUID")))){
                        this.subjectUUID = UUID.fromString(eventContent.getJSONObject("subject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                        this.predicateObjectUUID = UUID.fromString(eventContent.getJSONObject("predicateObject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                        String cmdOpen = eventContent.getJSONObject("properties").getJSONObject("map").getString("CommandLine");
                        return InitLog(this.processObjectCache.get(this.subjectUUID).getProcUUID().getPid(), this.processObjectCache.get(this.subjectUUID).getProcessName(), this.processObjectCache.get(this.predicateObjectUUID).getProcUUID().getPid(),this.processObjectCache.get(this.predicateObjectUUID).getProcessName(), cmdOpen);
                    }
                case "EVENT_FORK":
                    this.subjectUUID = UUID.fromString(eventContent.getJSONObject("subject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    this.predicateObjectUUID = UUID.fromString(eventContent.getJSONObject("predicateObject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    String cmd = eventContent.getJSONObject("properties").getJSONObject("map").getString("CommandLine");
                    return InitLog(this.processObjectCache.get(this.subjectUUID).getProcUUID().getPid(), this.processObjectCache.get(this.subjectUUID).getProcessName(), this.processObjectCache.get(this.predicateObjectUUID).getProcUUID().getPid(),this.processObjectCache.get(this.predicateObjectUUID).getProcessName(), cmd);
                case "EVENT_CONNECT":
                case "EVENT_SENDTO":
                case "EVENT_RECVFROM":
                    this.subjectUUID = UUID.fromString(eventContent.getJSONObject("subject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    this.predicateObjectUUID = UUID.fromString(eventContent.getJSONObject("predicateObject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
                    if(eventContent.getString("type").equals("RECVFROM"))
                        return InitLog(this.processObjectCache.get(this.subjectUUID).getProcUUID().getPid(), this.processObjectCache.get(this.subjectUUID).getProcessName(), this.predicateObjectUUID, PDM.NetEvent.Direction.IN);
                    else
                        return InitLog(this.processObjectCache.get(this.subjectUUID).getProcUUID().getPid(), this.processObjectCache.get(this.subjectUUID).getProcessName(), this.predicateObjectUUID, PDM.NetEvent.Direction.OUT);
                default:
                    return null;
            }
            }
        return null;
    }

}
