package ctg.workflow.sqlexporter;

import java.io.*;

/**
 * append
 */
public class FileProcessor {



    public static void processFile(String fileName) throws Exception {


        BufferedReader reader = null;
        FileReader fr = null;
        BufferedWriter bw = null;
        BufferedWriter writer = null;
        try {

            reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName),"UTF-8"));

            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(DBConfig.savepath + DBConfig.savefile, true),"UTF-8"));

            bw = new BufferedWriter(writer);

            while(true) {
                String nextLine = reader.readLine();

                if(nextLine == null) break;

                if(nextLine.startsWith("/*!")) {
                    continue;
                }

                bw.write(nextLine);
                bw.newLine();
            }
        } finally {
            if(fr != null) {
                fr.close();
            }
            if(reader != null) {
                reader.close();
            }
            if(bw != null) {
                bw.close();
            }
            if(writer != null) {
                writer.close();
            }
        }

    }


    public static void appendFile(String content) throws Exception {

        BufferedWriter bw = null;
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(DBConfig.savepath + DBConfig.savefile, true),"UTF-8"));

            bw = new BufferedWriter(writer);

            bw.write(content);
            bw.newLine();

        } finally {
            if(bw != null) {
                bw.close();
            }
            if(writer != null) {
                writer.close();
            }
        }

    }


}
