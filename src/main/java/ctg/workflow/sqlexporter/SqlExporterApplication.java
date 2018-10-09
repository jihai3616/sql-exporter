package ctg.workflow.sqlexporter;

import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.net.URLDecoder;

@SpringBootApplication
public class SqlExporterApplication {

    /**
     *
     * @param args
     */
    public static void main(String[] args) {

        ConfigurableApplicationContext context = SpringApplication.run(SqlExporterApplication.class, args);

        Environment environment = context.getBean(Environment.class);
        DBConfig.ip = environment.getProperty("db.config.ip");
        DBConfig.port = environment.getProperty("db.config.port");
        DBConfig.username = environment.getProperty("db.config.username");
        DBConfig.password = environment.getProperty("db.config.password");
        DBConfig.dbName = environment.getProperty("db.config.dbName");
        DBConfig.savepath = environment.getProperty("db.config.savepath");
        DBConfig.savefile = environment.getProperty("db.config.savefile");
        DBConfig.mysqldumpLocation = environment.getProperty("db.mysqldump.location");

        DBConfig.appId = environment.getProperty("db.config.appId");
        DBConfig.moduleId = environment.getProperty("db.config.moduleId");

        try {
            String path = ClassUtils.getDefaultClassLoader().getResource("").getPath();
            path = URLDecoder.decode(path, "utf-8");


            File path2 = new File(ResourceUtils.getURL("classpath:").getPath());
            System.out.println("path: " + path2.getAbsolutePath());
            if(!path2.exists()) {
                path2 = new File("");
            }

            //File[] files = path2.listFiles();
            System.out.println("path: " + path2.getAbsolutePath());
            File[] files = new File(path2.getAbsolutePath()).listFiles();
            if(files != null) {
                for(File file : files) {
                    //System.out.println(file);
                    if(file.isFile() && file.getName().contains("sql-exporter.jar")) {
                        DBConfig.mysqldumpLocation = path2.getAbsolutePath() + File.separator;
                        break;
                    }
                }
            }

            System.out.println("mysqldump location: " + DBConfig.mysqldumpLocation);
            //DBConfig.mysqldumpLocation = path2.getAbsolutePath() + File.separator;

        } catch (Exception e) {
            e.printStackTrace();
        }

        if(StringUtils.isEmpty(DBConfig.appId) && StringUtils.isEmpty(DBConfig.moduleId)) {
            System.out.println("appId 和 moduleId 都为空");

            boolean transfer = Boolean.parseBoolean(environment.getProperty("db.transfer.sys_organ"));
            if(!transfer) {
                return;
            }


            System.out.println("db.transfer.sys_organ设置为true，开始转移sys_user中organ数据");
            TransferSysOrgan transferSysOrgan = new TransferSysOrgan();
            transferSysOrgan.doTransfer();

            return;
        }

        DumpHelper helper = new DumpHelper();
        helper.start();

        if(StringUtils.isNotEmpty(DBConfig.moduleId)) {
            helper.dumpByModuleId(DBConfig.moduleId);
        } else if(StringUtils.isNotEmpty(DBConfig.appId)) {
            helper.dumpByAppId(DBConfig.appId);
        }

    }
}
