package LogParser.DRAPA;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import provenenceGraph.dataModel.PDM;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static LogParser.NodLink.win10LogParser.*;
import static provenenceGraph.dataModel.PDM.File.FileType.FILE_UNKNOWN;
import static provenenceGraph.dataModel.PDM.LogContent.*;
import static provenenceGraph.dataModel.PDM.LogContent.NET_CONNECT;

public class CADETSLogParser {
    public Long eventTimeStamp;
    private PDM.Log.Builder log_builder;
    private PDM.LogHeader uheader;
    public Map<UUID, PDM.NetEvent> netFlowCache;

    public CADETSLogParser() {
        this.netFlowCache = new HashMap<>();
    }

    public PDM.Log InitLog(Integer process_id, String process_name, String filepath){

        PDM.EventHeader eventHeader = setEventHeader(process_id, 0, process_name, eventTimeStamp);
        PDM.FileEvent fileEvent = setFileEvent(FILE_UNKNOWN, 0L, filepath);

        PDM.Log log = log_builder
                .setUHeader(uheader)
                .setEventData(
                        PDM.EventData.newBuilder()
                                .setEHeader(eventHeader)
                                .setFileEvent(fileEvent)
                )
                .build();
        return log;
    }

    public PDM.Log InitLog(Integer process_id, String process_name, UUID object, PDM.NetEvent.Direction direction){

        PDM.EventHeader eventHeader = setEventHeader(process_id, 0, process_name, eventTimeStamp);
        PDM.NetEvent netEventCache = netFlowCache.get(object);

        if (netEventCache == null) return null;
        PDM.NetEvent netEvent = setNetEvent(netEventCache.getSip(), netEventCache.getDip(), netEventCache.getSport(), netEventCache.getDport(), direction);
        PDM.Log log = log_builder
                .setUHeader(uheader)
                .setEventData(
                        PDM.EventData.newBuilder()
                                .setEHeader(eventHeader)
                                .setNetEvent(netEvent)
                )
                .build();
        return log;
    }

    public PDM.Log InitLog(Integer process_id, String process_name, String parent_process_name, String cmdLine){

        PDM.EventHeader eventHeader = setEventHeader(process_id, 0, process_name, eventTimeStamp);
        PDM.ProcessEvent processEvent = setProcessEvent(0,0, parent_process_name, cmdLine);

        PDM.Log log = log_builder
                .setUHeader(uheader)
                .setEventData(
                        PDM.EventData.newBuilder()
                                .setEHeader(eventHeader)
                                .setProcessEvent(processEvent)
                )
                .build();
        return log;
    }
    public PDM.Log jsonParse(String JsonData){
        //deSerialize
        JSONObject jsonobj;
        try {
            jsonobj = JSON.parseObject(JsonData);
        }catch (Exception e){
            return null;
        }
        log_builder = PDM.Log.newBuilder();
        //build LogHeader
        PDM.LogHeader.Builder uHeader_builder = PDM.LogHeader.newBuilder();
        uHeader_builder.setType(PDM.LogType.EVENT);

        PDM.LogContent logContent;
        String logCategory;
        PDM.NetEvent.Direction direction = PDM.NetEvent.Direction.NONE;
        JSONObject event = jsonobj.getJSONObject("datum").getJSONObject("com.bbn.tc.schema.avro.cdm18.Event");

        // 对于非事件数据： 收集 NetFlow事件
        if (event == null){
            JSONObject netFlow = jsonobj.getJSONObject("datum").getJSONObject("com.bbn.tc.schema.avro.cdm18.NetFlowObject");
            if (netFlow != null) {
                PDM.IPAddress sourceIP = convertToIP(netFlow.getString("localAddress"));
                if(sourceIP == null) return null;
                Integer sourcePort = netFlow.getInteger("localPort");

                PDM.IPAddress destinationIP = convertToIP(netFlow.getString("remoteAddress"));
                if(destinationIP == null) return null;
                Integer destinationPort = netFlow.getInteger("remotePort");

                PDM.NetEvent netEvent = setNetEvent(sourceIP, destinationIP, sourcePort, destinationPort, direction);
                this.netFlowCache.put(UUID.fromString(netFlow.getString("uuid")), netEvent);
            }
            return null;
        }

        String eventType = event.getString("type");

        switch (eventType) {
            case "EVENT_FORK":
                logContent = PROCESS_FORK;
                logCategory = "Process";
                break;
            case "EVENT_EXECUTE":
                logContent = PROCESS_EXEC;
                logCategory = "Process";
                break;
            case "EVENT_OPEN":
                logContent = FILE_OPEN;
                logCategory = "File";
                break;
            case "EVENT_READ":
                logContent = FILE_READ;
                logCategory = "File";
                break;
            case "EVENT_WRITE":
                logContent = FILE_WRITE;
                logCategory = "File";
                break;
            case "EVENT_RECVFROM":
                direction = PDM.NetEvent.Direction.IN;
                logContent = NET_CONNECT;
                logCategory = "Network";
                break;
            case "EVENT_SENDTO":
            case "EVENT_CONNECT":
                direction = PDM.NetEvent.Direction.OUT;
                logContent = NET_CONNECT;
                logCategory = "Network";
                break;
            default:
                return null;
        }

        //set logHeader
        long hostUUID= UUID.fromString(event.getString("hostId")).getLeastSignificantBits();

        uheader = uHeader_builder
                .setContent(logContent)
                .setClientID(PDM.HostUUID.newBuilder().setHostUUID(hostUUID).build()
                ).build();

        UUID object = null;
        try {
            object = UUID.fromString(event.getJSONObject("predicateObject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
        }catch (NullPointerException e){
            return null;
        }

        eventTimeStamp = event.getLong("timestampNanos");
        if (logCategory.equals("Process")) {
            String process_name = event.getJSONObject("properties").getJSONObject("map").getString("exec");
            int process_id = event.getJSONObject("properties").getJSONObject("map").getInteger("ppid");;

            String parent_process_name = (eventType.equals("EVENT_EXECUTE")) ? event.getJSONObject("predicateObjectPath").getString("string") : process_name;
            String cmdLine = (eventType.equals("EVENT_EXECUTE")) ? event.getJSONObject("properties").getJSONObject("map").getString("cmdLine") : "aue_fork";

            PDM.Log log = InitLog(process_id, process_name, parent_process_name, cmdLine);

            return log;
        }
        else if (logCategory.equals("File")) {
            String process_name = event.getJSONObject("properties").getJSONObject("map").getString("exec");
            int process_id = event.getJSONObject("properties").getJSONObject("map").getInteger("ppid");

            String filepath = event.getJSONObject("predicateObjectPath").getString("string");

            PDM.Log log = InitLog(process_id, process_name, filepath);

            return log;
        }
        else if (logCategory.equals("Network")) {
            String process_name = event.getJSONObject("properties").getJSONObject("map").getString("exec");
            int process_id = event.getJSONObject("properties").getJSONObject("map").getInteger("ppid");

            PDM.Log log = InitLog(process_id, process_name, object, direction);

            return log;
        }
        return null;
    }

}
