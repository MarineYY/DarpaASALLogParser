package kafkaProducer.Drapa;

import LogParser.DRAPA.darpaTRACELogParser;
import logSerialization.LogPackSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import provenenceGraph.dataModel.PDM;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;

public class darpaTRACELogPackProducer {

    public static int jsonCount = 0, logCount = 0, logPackCount = 1;

    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LogPackSerializer.class.getName());   // 配置键的序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LogPackSerializer.class.getName());   // 配置值的序列化器

        String folderPathTHEIA = "src/main/systemLog/trace/ta1-trace-e3-official-1.json/";
        System.out.println("start sending ...\n");
        for (int i = 0; i < 3; i ++) {
            File file = new File(folderPathTHEIA + "ta1-trace-e3-official-1.json." + i);
            if (i == 0) file = new File(folderPathTHEIA + "ta1-trace-e3-official-1.json");
            System.out.println("文件：  " + file.toString());
            sendLog(file, properties, "topic-TRACE-2");
        }
        System.out.println("end...");
    }

    public static void sendLog(File file, Properties properties, String topic) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String jsonline;
        //PDM.LogPack.Builder logpack_builder = PDM.LogPack.newBuilder();
        PDM.LogPack.Builder logpack_builder_train = PDM.LogPack.newBuilder();
        PDM.LogPack.Builder logpack_builder_test = PDM.LogPack.newBuilder();

        KafkaProducer<String, PDM.LogPack> kafkaProducer = new KafkaProducer<>(properties);

        darpaTRACELogParser darpaTRACELogParser = new darpaTRACELogParser();

        //逐行读取json文件,并进行序列化
        while ((jsonline = br.readLine()) != null) {
            jsonCount ++;

            if (jsonCount % 50 == 0){
                kafkaProducer.send(new ProducerRecord<>(topic + "-train", logpack_builder_train.build()));
                kafkaProducer.send(new ProducerRecord<>(topic + "-test", logpack_builder_test.build()));
                if (jsonCount % 300000 == 0) {
                    System.out.println("jsonCount: " + jsonCount);
                    System.out.println("logCount: " + logCount);
                    System.out.println("logPackCount: " + logPackCount);
                    System.out.println("continue...\n");
                }
                logPackCount ++;
                logpack_builder_train = PDM.LogPack.newBuilder();
                logpack_builder_test = PDM.LogPack.newBuilder();

            }

            //ArrayList<PDM.Log> logs = drapaCADETSLogParser.jsonParse(jsonline);
            ArrayList<PDM.Log> logs = darpaTRACELogParser.jsonParse(jsonline);
            try{
                for(PDM.Log log : logs) {
                    if(log.getEventData().getEHeader().getTs() < 1523633125720000000L){
                        logpack_builder_train.addData(log);
                    }
                    else{
                        logpack_builder_test.addData(log);
                    }
                    logCount++;
                }
            }catch (NullPointerException e){
                continue;
            }

        }

        kafkaProducer.send(new ProducerRecord<>(topic + "-train", logpack_builder_train.build()));
        kafkaProducer.send(new ProducerRecord<>(topic + "-test", logpack_builder_test.build()));
        System.out.println("jsonCount: " + jsonCount);
        System.out.println("logCount: " + logCount);
        System.out.println("logPackCount: " + logPackCount);
        br.close();
        kafkaProducer.close();
    }
}
