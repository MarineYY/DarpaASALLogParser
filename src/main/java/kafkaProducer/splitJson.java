package kafkaProducer;

import java.io.*;

public class splitJson {
    public static void main(String[] args) throws IOException {

//        File file = new File("src/main/systemLog/cadets/ta1-cadets-e3-official-2.json/ta1-cadets-e3-official-2.json.1");
        File file = new File("src/main/systemLog/cadets/apt2.log");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String jsonline;

        //逐行读取json文件,并进行序列化
        Integer cnt = 0;
        File file2 = new File("src/main/systemLog/cadets/apt3.log");
        BufferedWriter br2 = new BufferedWriter(new FileWriter(file2, true));
        System.out.println("start sending ...\n");
        while ((jsonline = br.readLine()) != null) {
            cnt ++;
            if (cnt >= 3220000){
                br2.write(jsonline);
                br2.newLine();
            }
        }

    }
}
