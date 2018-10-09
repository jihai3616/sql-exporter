package ctg.workflow.sqlexporter;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBHelper {

    private static String URL = "jdbc:mysql://" + DBConfig.ip + ":" + DBConfig.port + "/" + DBConfig.dbName
            + "?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8";

    /**
     * 只查数据库表一条记录中的一个字段
     * @param tableName 表名
     * @param conditionMap 查询条件
     * @param queryField 要查询的字段
     * @return
     * @throws Exception
     */
    public String queryField(String tableName, Map<String, String> conditionMap, String queryField) throws Exception{

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        String result = "";

        try {
            conn = DriverManager.getConnection(URL, DBConfig.username, DBConfig.password);
            st = conn.createStatement();
            StringBuilder sqlBuilder = new StringBuilder("select " + queryField + " from " + tableName + " where 1=1");

            if(conditionMap != null) {
                for (Map.Entry<String, String> entry : conditionMap.entrySet()) {
                    sqlBuilder.append(" and ");
                    sqlBuilder.append(entry.getKey()).append("='").append(entry.getValue()).append("'");
                }
            }

            //System.out.println(sqlBuilder.toString());
            rs = st.executeQuery(sqlBuilder.toString());

            while (rs.next()) {
                result = rs.getString(queryField);
                return result;
            }

        } catch (Exception e) {
            throw e;
        } finally {
            //关闭资源
            if(rs != null) {
                rs.close();
            }
            if(st != null) {
                st.close();
            }
            if(conn != null) {
                conn.close();
            }
        }

        return result;
    }

    /**
     * 只查数据库表一条记录中的多个字段
     * @param tableName 表名
     * @param conditionMap 查询条件
     * @param queryFields 要查询的字段
     * @return
     * @throws Exception
     */
    public Map<String, String> queryFields(String tableName, Map<String, String> conditionMap, List<String> queryFields) throws Exception{

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        Map<String, String> queryFieldMap = new HashMap<>();

        try {
            conn = DriverManager.getConnection(URL, DBConfig.username, DBConfig.password);
            int listSize = queryFields.size();

            StringBuilder sqlBuilder = new StringBuilder("select *");
            sqlBuilder.append(" from " + tableName + " where 1=1");

            if(conditionMap != null) {
                for (Map.Entry<String, String> entry : conditionMap.entrySet()) {
                    sqlBuilder.append(" and ");
                    sqlBuilder.append(entry.getKey()).append("='").append(entry.getValue()).append("'");
                }
            }

            st = conn.prepareStatement(sqlBuilder.toString());
            rs = st.executeQuery();


            //System.out.println("queryFields: app_id = " + conditionMap.entrySet().iterator().next().getValue());

            while (rs.next()) {
                for(int i=0; i<listSize; i++) {
                    queryFieldMap.put(queryFields.get(i), rs.getString(queryFields.get(i)));

                    //System.out.println("put " + queryFields.get(i) + ", value = " + rs.getString(queryFields.get(i)));
                }
                return queryFieldMap;
            }

        } catch (Exception e) {
            throw e;
        } finally {
            //关闭资源
            if(rs != null) {
                rs.close();
            }
            if(st != null) {
                st.close();
            }
            if(conn != null) {
                conn.close();
            }
        }

        return queryFieldMap;
    }

    public List<String> findAppIds(String tableName, String moduleId) throws Exception {

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        List<String> appIdList = new ArrayList<>();

        try {
            conn = DriverManager.getConnection(URL, DBConfig.username, DBConfig.password);
            st = conn.createStatement();
            String sql = "select id from " + tableName + " where module_id = '" + moduleId + "'";
            rs = st.executeQuery(sql);

            while (rs.next()) {
                appIdList.add(rs.getString("id"));
            }

        } catch (Exception e) {
            throw e;
        } finally {
            //关闭资源
            if(rs != null) {
                rs.close();
            }
            if(st != null) {
                st.close();
            }
            if(conn != null) {
                conn.close();
            }
        }

        return appIdList;
    }

    public String findModuleId(String tableName, String appId) throws Exception {

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;

        try {
            conn = DriverManager.getConnection(URL, DBConfig.username, DBConfig.password);
            st = conn.createStatement();
            String sql = "select module_id from " + tableName + " where id = '" + appId + "'";
            rs = st.executeQuery(sql);

            while (rs.next()) {
                return rs.getString("module_id");
            }

        } catch (Exception e) {
            throw e;
        } finally {
            //关闭资源
            if(rs != null) {
                rs.close();
            }
            if(st != null) {
                st.close();
            }
            if(conn != null) {
                conn.close();
            }
        }

        return "";
    }

    public List<String> checkControlType(String appId, String controlTableName, int type) throws Exception{

        List<String> subTableList = new ArrayList<>();

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            conn = DriverManager.getConnection(URL, DBConfig.username, DBConfig.password);
            String sql = "select * from " + controlTableName + " where app_id = ?";
            st = conn.prepareStatement(sql);
            st.setString(1, appId);
            rs = st.executeQuery();

            while (rs.next()) {
                if(rs.getInt("control_key") == type) {
                    subTableList.add(rs.getString("data_dictItem_name"));
                }
            }

        } catch (Exception e) {
            throw e;
        } finally {
            //关闭资源
            if(rs != null) {
                rs.close();
            }
            if(st != null) {
                st.close();
            }
            if(conn != null) {
                conn.close();
            }
        }

        return subTableList;
    }

    public int checkExist(String appId, String controlTableName) throws Exception{

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            conn = DriverManager.getConnection(URL, DBConfig.username, DBConfig.password);
            String sql = "select count(*) from " + controlTableName + " where app_id = ?";
            st = conn.prepareStatement(sql);
            st.setString(1, appId);
            rs = st.executeQuery();

            while (rs.next()) {
                return rs.getInt(1);
            }

        } catch (Exception e) {
            throw e;
        } finally {
            //关闭资源
            if(rs != null) {
                rs.close();
            }
            if(st != null) {
                st.close();
            }
            if(conn != null) {
                conn.close();
            }
        }

        return 0;
    }



    public void test() throws Exception{

        List<String> subTableList = new ArrayList<>();

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        String appId = "6cbff04adba240bf9e194cbc0381c1651096173435";

        /*String url = "jdbc:mysql://10.142.233.78:8601/workflow-demo2?useUnicode=true&characterEncoding=utf-8&autoReconnect=true";
        String username = "gw";
        String password = "gw123!@#";*/
        String url = "jdbc:mysql://127.0.0.1/workflow-demo2?useUnicode=true&characterEncoding=utf-8&autoReconnect=true";
        String username = "root";
        String password = "root";

        try {
            conn = DriverManager.getConnection(url, username, password);
            //String sql = "select app_id, display_name from re_wf_control_attribute where app_id = ?";

            String sql = "select count(1) as '应用数', " +
                    "FROM_UNIXTIME(substring(create_time,1,10),'%Y%m') month from ru_wf_task group by month";
            String sql2 = "SELECT count(1) AS '应用数', status as 状态, " +
                    "FROM_UNIXTIME( substring(create_time, 1, 10), '%Y%m' ) month FROM ru_wf_task  GROUP BY month, status";
            String sql3 = "INSERT INTO `sys_dict` VALUES ('19', null, 'PROJECT_PLAN_TIME', '3', null, '项目预计完成时间配置，单位为天');";

            st = conn.prepareStatement(sql2);
            //st.setString(1, appId);
            rs = st.executeQuery();

            final ResultSetMetaData rsMataData = rs.getMetaData();
            final int count = rsMataData.getColumnCount();
            for (int i = 1; i <= count; i++) {
                String columnName = rsMataData.getColumnName(i);
                String columnLabel = rsMataData.getColumnLabel(i);
                String columnTypeName = rsMataData.getColumnTypeName(i);
                int columnDisplaySize = rsMataData.getColumnDisplaySize(i);
                System.out.println(columnName + ", " + columnLabel + ", " + columnTypeName + ", " + columnDisplaySize);
            }

        } catch (Exception e) {
            throw e;
        } finally {
            //关闭资源
            if(rs != null) {
                rs.close();
            }
            if(st != null) {
                st.close();
            }
            if(conn != null) {
                conn.close();
            }
        }

    }

    public static void main(String[] args) throws Exception {
        new DBHelper().test();
    }


}
