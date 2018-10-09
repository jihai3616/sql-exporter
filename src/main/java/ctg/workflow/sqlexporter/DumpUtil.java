package ctg.workflow.sqlexporter;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class DumpUtil {

    /**
     * 容量设1就行了
     */
    public static BlockingQueue<String> delSqlQueue = new ArrayBlockingQueue<String>(5);

    public static void dumpSql(String tableName, String savePath, boolean includeData) throws Exception {
        dumpSql(tableName, savePath, includeData, null);
    }

    public static void dumpSql(String tableName, String savePath, boolean includeData, Map<String, String> conditionMap) throws Exception {

        if(StringUtils.isEmpty(tableName)) {
            System.out.println("tableName 为空");
            return;
        }

        File saveFile = new File(savePath);
        if (!saveFile.exists()) {  // 如果目录不存在，创建文件夹
            saveFile.mkdirs();
        }
        if (!savePath.endsWith(File.separator)) {
            savePath = savePath + File.separator;
        }

        StringBuilder stringBuilder = new StringBuilder();

        if(System.getProperty("os.name").toLowerCase().indexOf("linux") > 0) {
            stringBuilder.append("mysqldump ");
        } else {
            //stringBuilder.append("cmd /c C:/mysqldump");
            stringBuilder.append("cmd /c ").append(DBConfig.mysqldumpLocation).append("mysqldump");
        }

        if(!includeData) {
            stringBuilder.append(" --no-data");
        }

        if(tableName.startsWith("t_") || tableName.startsWith("sub_")) {

        } else {
            stringBuilder.append(" -t "); // 除了主表和子表，其他都只导数据
        }

        stringBuilder.append(" -h").append(DBConfig.ip)
                .append(" -P").append(DBConfig.port)
                .append(" -u").append(DBConfig.username)
                .append(" -p").append(DBConfig.password)
                .append(" " + DBConfig.dbName)
                .append(" " + tableName);

        //--add-locks 在每个表导出之前增加LOCK TABLES并且之后UNLOCK TABLE。
        // (默认为打开状态，使用--skip-add-locks取消选项)
        //stringBuilder.append(" --skip-add-locks ");
        //.append(" --lock-all-tables=true")
        stringBuilder.append(" --default-character-set=utf8 ");
        stringBuilder.append(" --skip-add-locks ");
        stringBuilder.append(" --set-gtid-purged=OFF ");

        if(conditionMap != null) {
            stringBuilder.append(" -w \"1=1");
            for (Map.Entry<String, String> entry : conditionMap.entrySet()) {
                stringBuilder.append(" and ");

                stringBuilder.append(entry.getKey()).append("='").append(entry.getValue()).append("'");
            }
            stringBuilder.append("\"");
        }

        String fileName = savePath + tableName + ".sql";
        stringBuilder.append(" --result-file=").append(fileName);

        System.out.println(stringBuilder.toString());

        try {
            Process process = Runtime.getRuntime().exec(stringBuilder.toString());
            // 0 表示线程正常终止。
            if (process.waitFor() == 0) {

                if(tableName.equals("sys_menu")) {
                    String sql = delSqlQueue.remove();
                    FileProcessor.appendFile(sql);
                }

                FileProcessor.processFile(fileName);  // 保存到一个文件

                return;
            } else {
                throw new Exception("导出" + tableName + "信息失败！");
            }
        } catch (Exception e) {
            throw e;
        }
    }


}
