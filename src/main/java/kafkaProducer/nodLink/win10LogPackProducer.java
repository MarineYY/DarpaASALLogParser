package kafkaProducer.nodLink;

import LogParser.NodLink.win10LogParser;
import logSerialization.LogPackSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import provenenceGraph.dataModel.PDM;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class win10LogPackProducer {

    public static int jsonCount = 0;
    public static int logCount = 0;
    public static int logPackCount = 0;

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.110:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LogPackSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LogPackSerializer.class.getName());

        /*
        Folder : win10/benign.json
         */
//        System.out.println("start sending ...\n");
//        File file = new File("src/main/systemLog/win10/benign.json");
//        sendLog(file, properties,"topic-win10-benign");
//        System.out.println("end...");

        /*
        Folder : win10/anomaly.json
         */
//        System.out.println("start sending ...\n");
//        File file = new File("src/main/systemLog/win10/anomaly.json");
//        sendLog(file, properties,"topic-win10-malicious");
//        System.out.println("end...");


        /*
        Folder : winServer2012/benign.json
         */
//        System.out.println("start sending ...\n");
//        File file = new File("src/main/systemLog/winServer2012/benign.json");
//        sendLog(file, properties,"topic-winS-benign");
//        System.out.println("end...");

        /*
        Folder : winServer2012/anomaly.json
         */
//        System.out.println("start sending ...\n");
//        File file = new File("src/main/systemLog/winServer2012/anomaly.json");
//        sendLog(file, properties,"topic-winS-malicious");
//        System.out.println("end...");
    }


    public static void sendLog(File file, Properties properties, String topic) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String jsonline;
        PDM.LogPack.Builder logpack_builder = PDM.LogPack.newBuilder();

        KafkaProducer<String, PDM.LogPack> kafkaProducer = new KafkaProducer<>(properties);
        win10LogParser win10LogParser = new win10LogParser();

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

            PDM.Log log = win10LogParser.jsonParse(jsonline);
            try{
                logpack_builder.addData(log);
                logCount++;
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
