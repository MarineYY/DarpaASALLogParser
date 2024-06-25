package kafkaProducer.nodLink;

import LogParser.NodLink.ubantuLogParser;
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

public class ubantuLogPackProducer {

    public static int jsonCount = 0;
    public static int logCount = 0;
    public static int logPackCount = 0;

    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.110:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LogPackSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LogPackSerializer.class.getName());

//        System.out.println("start sending ...\n");
//        File file = new File("D:/Program File/git_repository/dataFiles/ASAL/ubantu/benign.json");
//        sendLog(file, properties,"topic-ubantu");
//        System.out.println("end...");


        System.out.println("start sending ...\n");
        File file = new File("D:/Program File/git_repository/dataFiles/ASAL/ubantu/anomaly.json");
        sendLog(file, properties,"topic-ubantu");
        System.out.println("end...");
    }

    public static void sendLog(File file, Properties properties, String topic) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String jsonline;
        PDM.LogPack.Builder logpack_builder = PDM.LogPack.newBuilder();

        KafkaProducer<String, PDM.LogPack> kafkaProducer = new KafkaProducer<>(properties);
        ubantuLogParser ubantuLogParser = new ubantuLogParser();

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

            PDM.Log log = ubantuLogParser.jsonParse(jsonline);
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
