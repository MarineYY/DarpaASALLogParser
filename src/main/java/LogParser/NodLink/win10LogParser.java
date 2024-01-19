package LogParser.NodLink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import provenenceGraph.dataModel.PDM;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static kafkaProducer.nodLink.win10LogPackProducer.jsonCount;
import static provenenceGraph.dataModel.PDM.File.FileType.FILE_UNKNOWN;
import static provenenceGraph.dataModel.PDM.LogContent.*;

public class win10LogParser {

    public static Long eventTimeStamp;

    public static PDM.Log jsonParse(String JsonData){
        //deSerialize
        JSONObject jsonobj;

        try {
            jsonobj = JSON.parseObject(JsonData);
        } catch (Exception e) {
            return null;
        }
        if (jsonobj == null) return null;
        PDM.Log.Builder log_builder = PDM.Log.newBuilder();
        //build LogHeader
        PDM.LogHeader.Builder uHeader_builder = PDM.LogHeader.newBuilder();
        uHeader_builder.setType(PDM.LogType.EVENT);

        PDM.LogContent logContent;
        String logCategory;
        PDM.NetEvent.Direction direction = PDM.NetEvent.Direction.NONE;

        eventTimeStamp = (long)jsonCount;

        String eventName = jsonobj.getString("EventName");

        switch (eventName) {
            case "Process/Start":
            case "Process/Stop":
                logContent = PROCESS_EXEC;
                logCategory = "Process";
                break;
            case "Image/Load":
            case "FileIO/Read":
                logContent = FILE_READ;
                logCategory = "File";
                break;
            case "FileIO/Write":
                logContent = FILE_WRITE;
                logCategory = "File";
                break;
            case "TcpIp/Recv":
                direction = PDM.NetEvent.Direction.IN;
                logContent = NET_CONNECT;
                logCategory = "Network";
                break;
            case "TcpIp/Send":
                direction = PDM.NetEvent.Direction.OUT;
                logContent = NET_CONNECT;
                logCategory = "Network";
                break;
            default:
                return null;
        }

        uHeader_builder.setContent(logContent);

        //get hostuuid
        UUID uuid = UUID.nameUUIDFromBytes("192.168.0.74".getBytes(StandardCharsets.UTF_8));
        long hostUUID = uuid.getLeastSignificantBits();
        PDM.LogHeader uheader = uHeader_builder.setClientID(
                PDM.HostUUID.newBuilder().setHostUUID(hostUUID).build()
        ).build();

        if (logCategory.equals("Process")) {
            if (eventName.equals("Process/Stop")){
                String[] imageFileNames = jsonobj.getString("ImageFileName").split("\\.");

                String process_commandline = jsonobj.getString("CommandLine");
                String process_name = imageFileNames[0];
                String[] parentIDS = jsonobj.getString("ParentID").split(",");
                int process_id = Integer.parseInt(String.valueOf(parentIDS[0]));
                UUID process_uuid = UUID.nameUUIDFromBytes(process_name.getBytes(StandardCharsets.UTF_8));
                Long process_timestamp = process_uuid.getLeastSignificantBits();
                PDM.ProcessEvent processEvent = setProcessEvent(process_id, process_timestamp, process_name, process_commandline);


                int parent_process_id = jsonobj.getInteger("PID");
                String parent_process_name = imageFileNames[0];
                UUID parent_process_uuid = UUID.nameUUIDFromBytes(parent_process_name.getBytes(StandardCharsets.UTF_8));
                Long parent_process_timestamp = parent_process_uuid.getLeastSignificantBits();
                PDM.EventHeader eventHeader = setEventHeader(parent_process_id, parent_process_timestamp, parent_process_name, eventTimeStamp);
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

            String process_commandline = jsonobj.getString("CommandLine");
            String process_name = jsonobj.getString("PName");
            int process_id = jsonobj.getInteger("PID");
            UUID process_uuid = UUID.nameUUIDFromBytes(process_name.getBytes(StandardCharsets.UTF_8));
            Long process_timestamp = process_uuid.getLeastSignificantBits();


            String parentID = jsonobj.getString("ParentID");
            if (parentID == null) return null;
            int parent_process_id = Integer.parseInt(parentID);
            String parent_process_name = jsonobj.getString("PPName");
            UUID parent_process_uuid = UUID.nameUUIDFromBytes(parent_process_name.getBytes(StandardCharsets.UTF_8));
            Long parent_process_timestamp = parent_process_uuid.getLeastSignificantBits();

            // 设置eventheader --- subject
            PDM.EventHeader eventHeader = setEventHeader(parent_process_id, parent_process_timestamp, parent_process_name, eventTimeStamp);

            //设置processEvent  --- object
            PDM.ProcessEvent processEvent = setProcessEvent(process_id, process_timestamp, process_name, process_commandline);

            //设置 log
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
        else if (logCategory.equals("File")) {
            String process_name = jsonobj.getString("PName");
            if (process_name == null) return null;
            int process_id = Integer.parseInt(jsonobj.getString("PID"));
            UUID process_uuid = UUID.nameUUIDFromBytes(process_name.getBytes(StandardCharsets.UTF_8));
            Long process_timestamp = process_uuid.getLeastSignificantBits();

            String filepath = jsonobj.getString("FileName");
            long filePathHash = UUID.nameUUIDFromBytes(filepath.getBytes(StandardCharsets.UTF_8)).getLeastSignificantBits();

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
            return log;
        }
        else if (logCategory.equals("Network")) {
            String process_name = jsonobj.getString("PName");
            if (process_name == null) return null;
            int process_id = Integer.parseInt(jsonobj.getString("PID"));
            UUID process_uuid = UUID.nameUUIDFromBytes(process_name.getBytes(StandardCharsets.UTF_8));
            Long process_timestamp = process_uuid.getLeastSignificantBits();

            PDM.IPAddress sourceIP = convertToIP(jsonobj.getString("saddr"));
            PDM.IPAddress destinationIP = convertToIP(jsonobj.getString("daddr"));
            Integer sourcePort = convertToPort(jsonobj.getString("sport"));
            Integer destinationPort = convertToPort(jsonobj.getString("dport"));

            // 设置 Subject
            PDM.EventHeader eventHeader = setEventHeader(process_id, process_timestamp, process_name, eventTimeStamp);

            //设置object
            PDM.NetEvent netEvent = setNetEvent(sourceIP, destinationIP, sourcePort, destinationPort, direction);

            //设置 log
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

        return null;
    }

    public static PDM.IPAddress convertToIP(String remoteIp){
        if (remoteIp.equals("localhost")) remoteIp = "127.0.0.1";
        if (!remoteIp.contains(".") || remoteIp.contains("/"))return null;
        String[] splits = remoteIp.split("\\.");
        PDM.IPAddress ipAddress = PDM.IPAddress.newBuilder()
                .setAddress(Integer.parseInt(splits[0]))
                .setAddress1(Integer.parseInt(splits[1]))
                .setAddress2(Integer.parseInt(splits[2]))
                .setAddress3(Integer.parseInt(splits[3]))
                .build();

        return ipAddress;
    }

    public static Integer convertToPort(String port){
        String[] splits = port.split(",");
        int res = 0;
        for (String split : splits){
            res = res * 1000 + Integer.parseInt(split);
        }
        return res;
    }

    public static PDM.EventHeader setEventHeader(int processID, long processTimestamp, String processName, Long eventTimeStamp){
        PDM.Process proc_subject = PDM.Process.newBuilder()
                .setProcUUID(PDM.ProcessUUID.newBuilder().setPid(processID).setTs(processTimestamp))
                .setProcessName(processName)
                .setCmdline("")
                .build();

        PDM.EventHeader eventheader = PDM.EventHeader.newBuilder()
                .setProc(proc_subject)
                .setTs(eventTimeStamp)
                .build();
        return eventheader;
    }

    public static PDM.ProcessEvent setProcessEvent(int processID, long processTimestamp, String processName, String processCommandLine){
        PDM.Process proc_Object = PDM.Process.newBuilder()
                .setProcUUID(PDM.ProcessUUID.newBuilder().setPid(processID).setTs(processTimestamp).build())
                .setProcessName(processName)
                .setCmdline(processCommandLine)
                .build();
        PDM.ProcessEvent processEvent = PDM.ProcessEvent.newBuilder()
                .setChildProc(proc_Object)
                .build();
        return processEvent;
    }

    public static PDM.FileEvent setFileEvent(PDM.File.FileType fileType, long filePathHash, String filePath){
        PDM.FileEvent fileEvent = PDM.FileEvent.newBuilder()
                .setFile(
                        PDM.File.newBuilder()
                                .setFileType(fileType)
                                .setFileUUID(PDM.FileUUID.newBuilder().setFilePathHash(filePathHash).build())
                                .setFilePath(filePath)
                                .build()
                ).build();
        return fileEvent;
    }

    public static PDM.NetEvent setNetEvent(PDM.IPAddress sourceIp, PDM.IPAddress destinationIp, int sourcePort, int destinationPort, PDM.NetEvent.Direction direction){
        PDM.NetEvent netEvent = PDM.NetEvent.newBuilder()
                .setSip(sourceIp)
                .setDip(destinationIp)
                .setSport(sourcePort)
                .setDport(destinationPort)
                .setDirect(direction)
                .build();
        return netEvent;
    }
}
