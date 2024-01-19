package kafkaProducer.Drapa;

import LogParser.DRAPA.drapaCADETSLogParser;
import logSerialization.LogPackSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import provenenceGraph.dataModel.PDM;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;

public class drapaCADETSLogPackProducer {
    public static int jsonCount = 0, logCount = 0, logPackCount = 1;
    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.110:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LogPackSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LogPackSerializer.class.getName());

        //CADETS–NginxBackdoorw.log  topic-HipsToMrd
//        File file = new File("src/main/systemLog/cadets/BSD.log");
//        File file = new File("src/main/systemLog/cadets/ta1-cadets-e3-official-2.json/ta1-cadets-e3-official-2.json.1");
//        File file = new File("src/main/systemLog/cadets/apt.log");
//        sendLog(file, properties,"topic-HipsToMrd");

        /*
        Folder : ta1-cadets-e3-official.json
         */
//        String folderPathTHEIA = "src/main/systemLog/cadets/ta1-cadets-e3-official.json/";
//        System.out.println("start sending ...\n");
//        for (int i = 0; i < 3; i ++) {
//            File file = new File(folderPathTHEIA + "ta1-cadets-e3-official.json." + i);
//            if (i == 0) file = new File(folderPathTHEIA + "ta1-cadets-e3-official.json");
//            System.out.println("文件：  " + file.toString());
//            sendLog(file, properties, "topic-CADETS-0");
//        }
//        System.out.println("end...");


        /*
        Folder : ta1-cadets-e3-official.json
         */
//        String folderPathTHEIA = "src/main/systemLog/cadets/ta1-cadets-e3-official-1.json/";
//        System.out.println("start sending ...\n");
//        for (int i = 0; i < 5; i ++) {
//            File file = new File(folderPathTHEIA + "ta1-cadets-e3-official-1.json." + i);
//            if (i == 0) file = new File(folderPathTHEIA + "ta1-cadets-e3-official-1.json");
//            System.out.println("文件：  " + file.toString());
//            sendLog(file, properties, "topic-CADETS-1");
//        }
//        System.out.println("end...");

        /*
        Folder : ta1-cadets-e3-official.json
         */
//        String folderPathTHEIA = "src/main/systemLog/cadets/ta1-cadets-e3-official-2.json/";
//        System.out.println("start sending ...\n");
//        for (int i = 0; i < 2; i ++) {
//            File file = new File(folderPathTHEIA + "ta1-cadets-e3-official-2.json." + i);
//            if (i == 0) file = new File(folderPathTHEIA + "ta1-cadets-e3-official-2.json");
//            System.out.println("文件：  " + file.toString());
//            sendLog(file, properties, "topic-CADETS-2");
//        }
//        System.out.println("end...");
    }

    public static void sendLog(File file, Properties properties, String topic) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String jsonline;
        PDM.LogPack.Builder logpack_builder = PDM.LogPack.newBuilder();

        KafkaProducer<String, PDM.LogPack> kafkaProducer = new KafkaProducer<>(properties);
        drapaCADETSLogParser drapaCADETSLogParser = new drapaCADETSLogParser();

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

            ArrayList<PDM.Log> logs = drapaCADETSLogParser.jsonParse(jsonline);
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
