package kafkaProducer.Drapa;

import LogParser.DRAPA.drapaTHEIALogParser;
import logSerialization.LogPackSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import provenenceGraph.dataModel.PDM;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;

public class drapaTHEIALogPackProducer {
    public static int jsonCount = 0, logCount = 0, logPackCount = 1;
    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.110:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LogPackSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LogPackSerializer.class.getName());


//        sendLog(new File("src/systemLog/apt.log"), properties, "topic-test");
//        /*
//         * folder : src/main/systemLog/THEIA/ta1-theia-e3-official-1r.json
//         * */
//        String folderPathTHEIA = "D:/Program File/git_repository/dataFiles/darpa/theia/ta1-theia-e3-official-1r.json/";
//        System.out.println("start sending ...\n");
//        for (int i = 0; i < 10; i ++) {
//            File file = new File(folderPathTHEIA + "ta1-theia-e3-official-1r.json." + i);
//            if (i == 0) file = new File(folderPathTHEIA + "ta1-theia-e3-official-1r.json");
//            System.out.println("文件：  " + file.toString());
//            sendLog(file, properties, "topic-THEIA-1r");
//        }
//        System.out.println("end...");

        /*
         * folder : src/main/systemLog/THEIA/ta1-theia-e3-official-3.json
         * */
//        String folderPathTHEIA = "D:/Program File/git_repository/dataFiles/darpa/theia/ta1-theia-e3-official-3.json/";
//        System.out.println("start sending ...\n");
//        for (int i = 0; i < 1; i ++) {
//            File file = new File(folderPathTHEIA + "ta1-theia-e3-official-3.json." + i);
//            if (i == 0) file = new File(folderPathTHEIA + "ta1-theia-e3-official-3.json");
//            System.out.println("文件：  " + file.toString());
//            sendLog(file, properties, "topic-THEIA-1r");
//        }
//        System.out.println("end...");

        /*
         * folder : src/main/systemLog/THEIA/ta1-theia-e3-official-5m.json
         * */
//        String folderPathTHEIA = "D:/Program File/git_repository/dataFiles/darpa/theia/ta1-theia-e3-official-5m.json/";
//        System.out.println("start sending ...\n");
//        for (int i = 0; i < 1; i ++) {
//            File file = new File(folderPathTHEIA + "ta1-theia-e3-official-5m.json." + i);
//            if (i == 0) file = new File(folderPathTHEIA + "ta1-theia-e3-official-5m.json");
//            System.out.println("文件：  " + file.toString());
//            sendLog(file, properties, "topic-THEIA-1r");
//        }
//        System.out.println("end...");

        /*
        * folder : ta1-theia-e3-official-6r.json
        * */
        String folderPathTHEIA = "D:/Program File/git_repository/dataFiles/darpa/theia/ta1-theia-e3-official-6r.json/";
        System.out.println("start sending ...\n");
        for (int i = 0; i < 13; i ++) {
            File file = new File(folderPathTHEIA + "ta1-theia-e3-official-6r.json." + i);
            if (i == 0) file = new File(folderPathTHEIA + "ta1-theia-e3-official-6r.json");
            System.out.println("文件：  " + file.toString());
            sendLog(file, properties, "topic-THEIA-1r");
        }
        System.out.println("end...");


    }

    public static void sendLog(File file, Properties properties, String topic) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String jsonline;
        PDM.LogPack.Builder logpack_builder = PDM.LogPack.newBuilder();

        KafkaProducer<String, PDM.LogPack> kafkaProducer = new KafkaProducer<>(properties);
        drapaTHEIALogParser drapaTHEIALogParser = new drapaTHEIALogParser();

        //逐行读取json文件,并进行序列化
        while ((jsonline = br.readLine()) != null) {
            jsonCount ++;

            if (jsonCount % 50 == 0){
                kafkaProducer.send(new ProducerRecord<>(topic, logpack_builder.build()));
                if (jsonCount % 300000 == 0) {
                    System.out.println("jsonCount: " + jsonCount);
                    System.out.println("logCount: " + logCount);
                    System.out.println("logPackCount: " + logPackCount);
                    System.out.println("continue...\n");
                }
                logPackCount ++;
                logpack_builder = PDM.LogPack.newBuilder();

            }

            ArrayList<PDM.Log> logs = drapaTHEIALogParser.jsonParse(jsonline);
            try{
                for(PDM.Log log : logs) {
                    logpack_builder.addData(log);
                    logCount++;
                }
            }catch (NullPointerException e){
                continue;
            }

        }

        kafkaProducer.send(new ProducerRecord<>(topic, logpack_builder.build()));

        System.out.println("jsonCount: " + jsonCount);
        System.out.println("logCount: " + logCount);
        System.out.println("logPackCount: " + logPackCount);
        br.close();
        kafkaProducer.close();
    }
}
