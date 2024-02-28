package LogParser.DRAPA;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import provenenceGraph.dataModel.PDM;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static LogParser.NodLink.win10LogParser.*;
import static LogParser.NodLink.win10LogParser.setNetEvent;
import static provenenceGraph.dataModel.PDM.File.FileType.FILE_UNKNOWN;
import static provenenceGraph.dataModel.PDM.LogContent.*;
import static provenenceGraph.dataModel.PDM.LogContent.PROCESS_FORK;

public class darpaTRACELogParser {
    public Long eventTimeStamp;
    public Map<UUID, PDM.NetEvent> netFlowCache;
    private Map<UUID, UUID> parentProcessCache;
    private Map<UUID, String> processNameCache;

    public darpaTRACELogParser() {
        this.netFlowCache = new HashMap<>();
        this.parentProcessCache = new HashMap<>();
        this.processNameCache = new HashMap<>();
    }

    public ArrayList<PDM.Log> jsonParse(String JsonData){
        //deSerialize
        JSONObject jsonobj;
        try {
            jsonobj = JSON.parseObject(JsonData);
        }catch (Exception e){
            return null;
        }
        PDM.Log.Builder log_builder = PDM.Log.newBuilder();
        //build LogHeader
        PDM.LogHeader.Builder uHeader_builder = PDM.LogHeader.newBuilder();
        uHeader_builder.setType(PDM.LogType.EVENT);

        PDM.LogContent logContent;
        String logCategory;
        PDM.NetEvent.Direction direction = PDM.NetEvent.Direction.NONE;
        JSONObject event = jsonobj.getJSONObject("datum").getJSONObject("com.bbn.tc.schema.avro.cdm18.Event");

        // 对于非事件数据： 收集 NetFlow事件  和  Process事件（父进程信息）
        if (event == null){
            JSONObject netFlow = jsonobj.getJSONObject("datum").getJSONObject("com.bbn.tc.schema.avro.cdm18.NetFlowObject");
            JSONObject process = jsonobj.getJSONObject("datum").getJSONObject("com.bbn.tc.schema.avro.cdm18.Subject");
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
            else if (process != null) {
                if (process.getJSONObject("parentSubject") != null) {
                    parentProcessCache.put(UUID.fromString(process.getString("uuid")), UUID.fromString(process.getJSONObject("parentSubject").getString("com.bbn.tc.schema.avro.cdm18.UUID")));
                }
                processNameCache.put(UUID.fromString(process.getString("uuid")), "unknown");
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
                logContent = PROCESS_LOAD;
                logCategory = "Process";
                break;
//            case "EVENT_OPEN":
//                logContent = FILE_OPEN;
//                logCategory = "File";
//                break;
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

        PDM.LogHeader uheader = uHeader_builder
                .setContent(logContent)
                .setClientID(PDM.HostUUID.newBuilder().setHostUUID(hostUUID).build()
                ).build();

        ArrayList<PDM.Log> logList = new ArrayList<>();
        UUID subject = UUID.fromString(event.getJSONObject("subject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
        UUID object = null;
        try {
            object = UUID.fromString(event.getJSONObject("predicateObject").getString("com.bbn.tc.schema.avro.cdm18.UUID"));
        }catch (NullPointerException e){
//            System.out.println(JsonData);
            return null;
        }
        UUID newSubject = UUID.nameUUIDFromBytes(subject.toString().getBytes(StandardCharsets.UTF_8));
        UUID newObject = UUID.nameUUIDFromBytes(object.toString().getBytes(StandardCharsets.UTF_8));

        if (logCategory.equals("Process")) {

            // return 了
            if (logContent == PROCESS_FORK) {
                // add by wangjiahui 2024.2.27
                // 因为trace数据集里这个字段全部为空，所以设为unknown？
                String process_name = event.getJSONObject("properties").getJSONObject("map").getString("exec");
                if(process_name == null){
                    process_name = "unknown";
                }
                // end by wangjiahui 2024.2.27

                parentProcessCache.put(object, subject);
                processNameCache.put(subject, process_name);
                processNameCache.put(object, process_name);
                PDM.LogHeader uheaderFork = uHeader_builder
                        .setContent(PROCESS_FORK)
                        .setClientID(PDM.HostUUID.newBuilder().setHostUUID(hostUUID).build()
                        ).build();
                PDM.EventHeader eventHeaderFork = setEventHeader((int) newSubject.getLeastSignificantBits(), newSubject.getMostSignificantBits(), process_name, 0L);
                PDM.ProcessEvent processEventFork = setProcessEvent((int) newObject.getLeastSignificantBits(), newObject.getMostSignificantBits(), process_name, "fork");
                PDM.Log logFork = log_builder
                        .setUHeader(uheaderFork)
                        .setEventData(
                                PDM.EventData.newBuilder()
                                        .setEHeader(eventHeaderFork)
                                        .setProcessEvent(processEventFork)
                        )
                        .build();
                logList.add(logFork);
                return logList;
            }

            // add by wangjiahui 2024.2.27
            String process_name;
            try {
                process_name = event.getJSONObject("predicateObjectPath").getString("string");
            }catch (NullPointerException e){
                process_name = "unknown";
            }
            //end by wangjiahui 2024.2.27

            int process_id = (int) newSubject.getLeastSignificantBits();
            Long process_timestamp = newSubject.getMostSignificantBits();
            eventTimeStamp = event.getLong("timestampNanos");

            String filepath = process_name;
            Long filePathHash = newObject.getMostSignificantBits();

            // 设置 Subject
            PDM.EventHeader eventHeader = setEventHeader(process_id, process_timestamp, process_name, eventTimeStamp);
            //设置 Object
            PDM.FileEvent fileEvent = setFileEvent(FILE_UNKNOWN, filePathHash, filepath);

            //设置 log
            PDM.Log log = log_builder
                    .setUHeader(uheader)
                    .setEventData(
                            PDM.EventData.newBuilder()
                                    .setEHeader(eventHeader)
                                    .setFileEvent(fileEvent)
                    )
                    .build();
            // set FORK
            UUID sonUUID = newSubject;
            UUID parentUUID = null;
            try {
                parentUUID = UUID.nameUUIDFromBytes(parentProcessCache.get(subject).toString().getBytes(StandardCharsets.UTF_8));
            }catch (NullPointerException e){
                logList.add(log);
                return logList;
            }
            PDM.LogHeader uheaderFork = uHeader_builder
                    .setContent(PROCESS_FORK)
                    .setClientID(PDM.HostUUID.newBuilder().setHostUUID(hostUUID).build()
                    ).build();

            String parent_process_name = processNameCache.get(parentProcessCache.get(subject));
            if (parent_process_name == null) {
                //process_name = "unknown";
                parent_process_name = "unknown";
            }
            PDM.EventHeader eventHeaderFork = setEventHeader((int) parentUUID.getLeastSignificantBits(), parentUUID.getMostSignificantBits(), parent_process_name, 0L);
            PDM.ProcessEvent processEventFork = setProcessEvent((int) sonUUID.getLeastSignificantBits(), sonUUID.getMostSignificantBits(), process_name, "fork");
            PDM.Log logFork = log_builder
                    .setUHeader(uheaderFork)
                    .setEventData(
                            PDM.EventData.newBuilder()
                                    .setEHeader(eventHeaderFork)
                                    .setProcessEvent(processEventFork)
                    )
                    .build();

            logList.add(logFork);
            logList.add(log);
            processNameCache.put(subject, process_name);
            return logList;
        }
        else if (logCategory.equals("File")) {

            //add by wangjiahui 2024.2.27
            String process_name = event.getJSONObject("properties").getJSONObject("map").getString("exec");
            if(process_name == null){
                process_name = "unknown";
            }
            //end by wangjiahui 2024.2.27

            processNameCache.put(subject, process_name);
            int process_id = (int) newSubject.getLeastSignificantBits();
            Long process_timestamp = newSubject.getMostSignificantBits();
            eventTimeStamp = event.getLong("timestampNanos");

            //add by wangjiahui 2024.2.27
            String filepath;
            try {
                filepath = event.getJSONObject("predicateObjectPath").getString("string");
            }catch (NullPointerException e){
                filepath = "unknown";
            }
            //end by wangjiahui 2024.2.27

            Long filePathHash = newObject.getMostSignificantBits();

            // 设置 Subject
            PDM.EventHeader eventHeader = setEventHeader(process_id, process_timestamp, process_name, eventTimeStamp);
            //设置 Object
            PDM.FileEvent fileEvent = setFileEvent(FILE_UNKNOWN, filePathHash, filepath);

            //设置 log
            PDM.Log log = log_builder
                    .setUHeader(uheader)
                    .setEventData(
                            PDM.EventData.newBuilder()
                                    .setEHeader(eventHeader)
                                    .setFileEvent(fileEvent)
                    )
                    .build();
            logList.add(log);
            return logList;
        }
        else if (logCategory.equals("Network")) {

            //add by wangjiahui 2024.2.27
            String process_name = event.getJSONObject("properties").getJSONObject("map").getString("exec");
            if(process_name == null){
                process_name = "unknown";
            }
            //end by wangjiahui 2024.2.27

            processNameCache.put(subject, process_name);
            int process_id = (int) newSubject.getLeastSignificantBits();
            Long process_timestamp = newSubject.getMostSignificantBits();
            eventTimeStamp = event.getLong("timestampNanos");

            // 设置 Subject
            PDM.EventHeader eventHeader = setEventHeader(process_id, process_timestamp, process_name, eventTimeStamp);

            //设置object
            PDM.NetEvent netEventCache = netFlowCache.get(object);
            if (netEventCache == null) return null;
            PDM.NetEvent netEvent = setNetEvent(netEventCache.getSip(), netEventCache.getDip(), netEventCache.getSport(), netEventCache.getDport(), direction);
            //设置 log
            PDM.Log log = log_builder
                    .setUHeader(uheader)
                    .setEventData(
                            PDM.EventData.newBuilder()
                                    .setEHeader(eventHeader)
                                    .setNetEvent(netEvent)
                    )
                    .build();
            logList.add(log);
            return logList;
        }

        return null;
    }
}
