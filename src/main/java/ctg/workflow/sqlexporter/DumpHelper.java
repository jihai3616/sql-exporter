package ctg.workflow.sqlexporter;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DumpHelper {

    /*public static void main(String[] args) throws Exception {
        DumpHelper helper = new DumpHelper();
        DBHelper dbHelper = new DBHelper();

        //DumpUtil.dumpSql("re_wf_app", Constant.SAVEPATH, false);

        //helper.dumpByAppId("2e4fb348e8ff423ca2b81daf5cc7188a1565331287");

        //helper.dumpByModuleId("644c503475844d3c8184aedafae4dec1");

    }*/


    public void start() {

        System.out.println("----------------------------- helper started");

    }

    public void dumpByModuleId(String moduleId) {

        DBHelper dbHelper = new DBHelper();

        String[] moduleIds = moduleId.split(",");

        checkSaveFile();

        try {

            FileProcessor.appendFile("set FOREIGN_KEY_CHECKS=0;");

            for(String mId : moduleIds) {
                mId = mId.trim();
                String savePath = DBConfig.savepath + "module_" + mId + File.separator;

                Map<String, String> idCondMap = new HashMap<>();
                idCondMap.put("id", mId);
                DumpUtil.dumpSql("re_wf_module", savePath, true, idCondMap);

                List<String> appIds = dbHelper.findAppIds("re_wf_app", mId);

                for(String appId : appIds) {
                    dumpByAppId(appId, dbHelper, savePath + "app_" + appId + File.separator);
                }

            }

            FileProcessor.appendFile("set FOREIGN_KEY_CHECKS=1;");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void checkSaveFile() {

        File file = new File(DBConfig.savepath + DBConfig.savefile);
        if(file.exists()) {
            file.delete();
        }
    }

    public void dumpByAppId(String appId) {

        DBHelper dbHelper = new DBHelper();

        String[] appIds = appId.split(",");

        checkSaveFile();

        try {

            FileProcessor.appendFile("set FOREIGN_KEY_CHECKS=0;");


            for(String aId : appIds) {
                aId = aId.trim();
                // 先取出module数据并导出
                String moduleId = dbHelper.findModuleId("re_wf_app", aId);

                String savePath = DBConfig.savepath + "module_" + moduleId + File.separator;

                File saveFile = new File(savePath);
                if (!saveFile.exists()) {  // 如果目录不存在，创建文件夹
                    saveFile.mkdirs();
                }
                //Map<String, String> idCondMap = new HashMap<>();
                //idCondMap.put("id", moduleId);
                //DumpUtil.dumpSql("re_wf_module", savePath, true, idCondMap);

                // 再导app数据
                dumpByAppId(aId, dbHelper, savePath + "app_" + aId + File.separator);

            }

            FileProcessor.appendFile("set FOREIGN_KEY_CHECKS=1;");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void dumpByAppId(String appId, DBHelper dbHelper, String savePath) {

        try {

            if(dbHelper == null) {
                dbHelper = new DBHelper();
            }

            // re_wf_app
            Map<String, String> idCondMap = new HashMap<>();
            idCondMap.put("id", appId);
            DumpUtil.dumpSql("re_wf_app", savePath, true, idCondMap);
            String formTableName = dbHelper.queryField("re_wf_app", idCondMap, "form_table_name");


            // re_wf_app 中取出的主表，不一定存在，判断一下
            if(StringUtils.isNotEmpty(formTableName)) {
                DumpUtil.dumpSql(formTableName, savePath, false);
            }


            // re_wf_control_attribute 表单控件属性表
            Map<String, String> appIdCondMap = new HashMap<>();
            appIdCondMap.put("app_id", appId);
            DumpUtil.dumpSql("re_wf_control_attribute", savePath, true, appIdCondMap);

            // 检查 re_wf_control_attribute 中是否有 control_key = 41，
            // 有则根据 re_wf_control_attribute.data_dictItem_name 取出子表
            List<String> subFormTableList = dbHelper.checkControlType(appId, "re_wf_control_attribute", 41);
            subFormTableList.stream().forEach(subFormTable -> {
                try {
                    DumpUtil.dumpSql(subFormTable, savePath, false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });


            // re_wf_list_view_setting，列表设计，这个要进去设置了才有，所以可以判断一下
            int count = dbHelper.checkExist(appId, "re_wf_list_view_setting");
            if(count > 0) {
                DumpUtil.dumpSql("re_wf_list_view_setting", savePath, true, appIdCondMap);
            }


            // re_wf_model 流程模型表
            String modelId = dbHelper.queryField("re_wf_app", idCondMap, "model_id");
            if(StringUtils.isNotEmpty(modelId)) {
                Map<String, String> modelIdCondMap = new HashMap<>();
                modelIdCondMap.put("model_id", modelId);
                DumpUtil.dumpSql("re_wf_model", savePath, true, modelIdCondMap);

                // re_wf_node 流程节点信息表
                // re_wf_nodeline 线节点表
                // re_wf_nodeuser 用户任务节点表
                // re_wf_subprocess_rule
                DumpUtil.dumpSql("re_wf_node", savePath, true, modelIdCondMap);
                DumpUtil.dumpSql("re_wf_nodeline", savePath, true, modelIdCondMap);   // 可能没值
                DumpUtil.dumpSql("re_wf_nodeuser", savePath, true, modelIdCondMap);
                DumpUtil.dumpSql("re_wf_subprocess_rule", savePath, true, modelIdCondMap);  // 可能没值



                // 再根据 modelId 从 re_wf_model 中取出 deployment_id，defid
                List<String> modelQuerylist = Arrays.asList("deployment_id", "defid");
                Map<String, String> modelMap = dbHelper.queryFields("re_wf_model", modelIdCondMap, modelQuerylist);



                // 导出引擎表的配置数据
                // act_re_deployment   ID_= re_wf_model.deployment_id
                // act_ge_bytearray    DEPLOYMENT_ID_ = re_wf_model.deployment_id
                // act_re_model        ID_ = re_wf_model.model_id
                // act_re_procdef      ID_ = re_wf_model.defid
                Map<String, String> activitiCondMap = new HashMap<>();
                if(modelMap.size() > 0) {

                    activitiCondMap.clear();
                    activitiCondMap.put("ID_", modelMap.get("deployment_id"));
                    DumpUtil.dumpSql("act_re_deployment", savePath, true, activitiCondMap);

                    activitiCondMap.clear();
                    activitiCondMap.put("DEPLOYMENT_ID_", modelMap.get("deployment_id"));
                    DumpUtil.dumpSql("act_ge_bytearray", savePath, true, activitiCondMap);

                    activitiCondMap.clear();
                    activitiCondMap.put("ID_", modelMap.get("defid"));
                    DumpUtil.dumpSql("act_re_procdef", savePath, true, activitiCondMap);

                }

                // 由于外键原因，act_re_model要在act_re_deployment之后导
                activitiCondMap.clear();
                activitiCondMap.put("ID_", modelId);
                DumpUtil.dumpSql("act_re_model", savePath, true, activitiCondMap);

            }

            String appKey = dbHelper.queryField("re_wf_app", idCondMap, "app_key");
            Map<String, String> appKeyMap = new HashMap<>();
            appKeyMap.put("resource_id", appKey);

            DumpUtil.delSqlQueue.add("delete from sys_menu where resource_id = '" + appKey + "';");

            DumpUtil.dumpSql("sys_menu", savePath, true, appKeyMap);


        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

}
