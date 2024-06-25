package LogParser.NodLink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple4;
import provenenceGraph.dataModel.PDM;

import java.lang.String;

import static provenenceGraph.dataModel.PDM.LogContent.*;

public class ubantuLogParser {
    public PDM.Log jsonParse(String JsonData) throws IOException {
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
        String event_type = jsonobj.getString("evt.type");
        PDM.NetEvent.Direction direction = PDM.NetEvent.Direction.NONE;
        String logCategory;
        switch (event_type) {
            case "fork":
            case "clone":
                logContent = PROCESS_FORK;
                logCategory = "Process";
                break;
            case "execve":
                logContent = PROCESS_EXEC;
                logCategory = "Process";
                break;
            case "read":
            case "readv":
                logContent = FILE_READ;
                logCategory = "File";
                break;
            case "write":
            case "writev":
                logContent = FILE_WRITE;
                logCategory = "File";
                break;
            case "recvmsg":
            case "recvfrom":
                direction = PDM.NetEvent.Direction.IN;
                logContent = NET_CONNECT;
                logCategory = "Network";
                break;
            case "sendmsg":
            case "send":
            case "sendto":
                direction = PDM.NetEvent.Direction.OUT;
                logContent = NET_CONNECT;
                logCategory = "Network";
                break;
            default:
                return null;
        }
        uHeader_builder.setContent(logContent);

        //get hostuuid
        UUID uuid = UUID.nameUUIDFromBytes("127.0.0.1".getBytes(StandardCharsets.UTF_8));
        long hostUUID = uuid.getLeastSignificantBits();
        PDM.LogHeader uheader = uHeader_builder.setClientID(
                PDM.HostUUID.newBuilder().setHostUUID(hostUUID).build()
        ).build();

        PDM.EventHeader.Builder eventheader_builder = PDM.EventHeader.newBuilder();
        Long eventTimeStamp = jsonobj.getLong("evt.time");
        if (logCategory.equals("Process")) {

            String process_commandline = jsonobj.getString("proc.cmdline");
            String process_name = jsonobj.getString("proc.name");
            String parent_process_commandline = jsonobj.getString("proc.pcmdline");
            String parent_process_name = jsonobj.getString("proc.pname");

            if (parent_process_commandline == null) return null;
            UUID parent_process_uuid = UUID.nameUUIDFromBytes(parent_process_commandline.getBytes(StandardCharsets.UTF_8));
            int parent_process_id = parent_process_uuid.hashCode();
            Long parent_process_timestamp = parent_process_uuid.getLeastSignificantBits();

            UUID process_uuid = UUID.nameUUIDFromBytes(process_commandline.getBytes(StandardCharsets.UTF_8));
            int process_id = process_uuid.hashCode();
            Long process_timestamp = process_uuid.getLeastSignificantBits();

            // 设置eventheader --- subject
            PDM.Process proc_subject = PDM.Process.newBuilder()
                    .setProcUUID(PDM.ProcessUUID.newBuilder().setPid(parent_process_id).setTs(parent_process_timestamp))
                    .setProcessName(parent_process_name)
                    .setCmdline(parent_process_commandline)
                    .build();

            PDM.EventHeader eventheader = eventheader_builder
                    .setProc(proc_subject)
                    .setTs(eventTimeStamp)
                    .build();


            //设置processEvent  --- object
            PDM.Process proc_Object = PDM.Process.newBuilder()
                    .setProcUUID(PDM.ProcessUUID.newBuilder().setPid(process_id).setTs(process_timestamp).build())
                    .setProcessName(process_name)
                    .setCmdline(process_commandline)
                    .build();
            PDM.ProcessEvent processEvent = PDM.ProcessEvent.newBuilder()
                    .setChildProc(proc_Object)
                    .build();

            //设置 log
            PDM.Log log = log_builder
                    .setUHeader(uheader)
                    .setEventData(
                            PDM.EventData.newBuilder()
                                    .setEHeader(eventheader)
                                    .setProcessEvent(processEvent)
                    )
                    .build();
            return log;
        } else if (logCategory.equals("File")) {
            String process_commandline = jsonobj.getString("proc.cmdline");
            String process_name = jsonobj.getString("proc.name");
            String filepath = jsonobj.getString("fd.name");
            if (filepath == null) return null;

            UUID process_uuid = UUID.nameUUIDFromBytes(process_commandline.getBytes(StandardCharsets.UTF_8));
            int process_id = process_uuid.hashCode();
            Long process_timestamp = process_uuid.getLeastSignificantBits();

            UUID file_uuid = UUID.nameUUIDFromBytes(filepath.getBytes(StandardCharsets.UTF_8));
            // get least 64 bits
            long filePathHash = file_uuid.getLeastSignificantBits();

            // 设置 Subject
            PDM.Process proc_subject = PDM.Process.newBuilder()
                    .setProcUUID(PDM.ProcessUUID.newBuilder().setPid(process_id).setTs(process_timestamp).build())
                    .setProcessName(process_name)
                    .setCmdline(process_commandline)
                    .build();
            PDM.EventHeader eventheader = eventheader_builder
                    .setTs(eventTimeStamp)
                    .setProc(proc_subject)
                    .build();
            //设置 Object
            PDM.FileEvent.Builder fileevent_builder = PDM.FileEvent.newBuilder();
            PDM.FileEvent fileEvent = fileevent_builder
                    .setFile(
                            PDM.File.newBuilder()
                                    .setFileType(PDM.File.FileType.FILE_UNKNOWN)
                                    .setFileUUID(PDM.FileUUID.newBuilder().setFilePathHash(filePathHash).build())
                                    .setFilePath(filepath)
                                    .build()
                    ).build();

            //设置 log
            PDM.Log log = log_builder
                    .setUHeader(uheader)
                    .setEventData(
                            PDM.EventData.newBuilder()
                                    .setEHeader(eventheader)
                                    .setFileEvent(fileEvent)
                    )
                    .build();
            return log;
        } else if (logCategory.equals("Network")) {
            String process_commandline = jsonobj.getString("proc.cmdline");
            String process_name = jsonobj.getString("proc.name");
            String networkInfo = jsonobj.getString("fd.name");
            if (networkInfo == null) return null;
            if(networkInfo.indexOf(":") < 0 || networkInfo.indexOf("->") < 0) return null;

            UUID process_uuid = UUID.nameUUIDFromBytes(process_commandline.getBytes(StandardCharsets.UTF_8));
            int process_id = process_uuid.hashCode();
            Long process_timestamp = process_uuid.getLeastSignificantBits();
            Tuple4<Integer, Integer, Integer, Integer> network = getNetworkInfo(networkInfo);

            // 设置 Subject
            PDM.Process proc_subject = PDM.Process.newBuilder()
                    .setProcUUID(PDM.ProcessUUID.newBuilder().setPid(process_id).setTs(process_timestamp).build())
                    .setProcessName(process_name)
                    .setCmdline(process_commandline)
                    .build();
            PDM.EventHeader eventheader = eventheader_builder
                    .setTs(eventTimeStamp)
                    .setProc(proc_subject)
                    .build();

            //设置object
            PDM.NetEvent.Builder netevent_builder = PDM.NetEvent.newBuilder();
            PDM.NetEvent netEvent = netevent_builder
                    .setSip(PDM.IPAddress.newBuilder().setAddress(network.f0).build())
                    .setDip(PDM.IPAddress.newBuilder().setAddress(network.f2).build())
                    .setSport(network.f1)
                    .setDport(network.f3)
                    .setDirect(direction)
                    .build();

            //设置 log
            PDM.Log log = log_builder
                    .setUHeader(uheader)
                    .setEventData(
                            PDM.EventData.newBuilder()
                                    .setEHeader(eventheader)
                                    .setNetEvent(netEvent)
                    )
                    .build();
            return log;
        }

        return null;
    }

        public static Tuple4<Integer, Integer, Integer, Integer> getNetworkInfo(String networkInfo){

            Integer sip = convertToDip(networkInfo.substring(0, networkInfo.indexOf(":")));
            Integer source_port = Integer.parseInt(networkInfo.substring(networkInfo.indexOf(":") + 1, networkInfo.indexOf("-")));
            String dipstring = networkInfo.substring(networkInfo.indexOf(">") + 1);
            Integer dip = convertToDip(dipstring.substring(0, dipstring.indexOf(":")));
            Integer dest_port = Integer.parseInt(dipstring.substring(dipstring.indexOf(":") + 1));
            return Tuple4.of(sip, source_port, dip, dest_port);
        }

        public static Integer convertToDip(String remoteIp){
            String[] splits = remoteIp.split("\\.");
            String res = "";
            for (String str : splits){
                res += str;
            }
            return (int)((Long.parseLong(res)) % 200000000);
        }
    }

