package ctg.workflow.sqlexporter;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TransferSysOrgan {

    private static String URL = "jdbc:mysql://" + DBConfig.ip + ":" + DBConfig.port + "/" + DBConfig.dbName
            + "?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8";

    public void doTransfer() {
        try {

            // 1、查id, user_name, organ_id
            List<UserBean> userList = queryList();


            // 2、判断 sys_user_organ 表是否存在，没有则创建
            boolean exist = validateTableExist("sys_user_organ");
            if(!exist) {
                System.out.println("sys_user_organ 表不存在，开始新建");

                createTable();
            }


            // 3、插入数据
            System.out.println("开始插入数据到 sys_user_organ 表，共" + userList.size() + "条");
            insertList(userList);
            System.out.println("插入数据到 sys_user_organ 表完成");

        } catch (Exception e) {
            System.out.println("出错了。。。");
            e.printStackTrace();
        }
    }


    private List<UserBean> queryList() throws Exception {

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;

        List<UserBean> resultList = new ArrayList<>();

        try {
            conn = DriverManager.getConnection(URL, DBConfig.username, DBConfig.password);
            st = conn.createStatement();
            String sql = "select id, user_name, organ_id from sys_user";
            rs = st.executeQuery(sql);

            while (rs.next()) {
                UserBean user = new UserBean();
                user.setId(rs.getString("id"));
                user.setUsername(rs.getString("user_name"));
                user.setOrganId(rs.getString("organ_id"));
                resultList.add(user);
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

        return resultList;
    }

    private void createTable() throws Exception {

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        Statement st = null;

        try {
            conn = DriverManager.getConnection(URL, DBConfig.username, DBConfig.password);
            st = conn.createStatement();

            StringBuilder sBuilder = new StringBuilder("CREATE TABLE ")
                    .append("sys_user_organ ( ")
                    .append("  user_id varchar(32) NOT NULL, ")
                    .append("  organ_id varchar(32) NOT NULL ")
                    .append(") ENGINE=InnoDB DEFAULT CHARSET=utf8; ");

            int i = st.executeUpdate(sBuilder.toString()); //DDL语句返回值为0;
            if (i == 0) {
                System.out.println("sys_user_organ 表创建成功！");
            }

        } catch (Exception e) {
            throw e;
        } finally {
            //关闭资源
            if(st != null) {
                st.close();
            }
            if(conn != null) {
                conn.close();
            }
        }
    }

    private void insertList(List<UserBean> userList) throws Exception {

        if(userList.isEmpty()) {
            return;
        }

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        PreparedStatement pst = null;

        try {
            conn = DriverManager.getConnection(URL, DBConfig.username, DBConfig.password);

            String sql = "insert into sys_user_organ(user_id, organ_id) values(?, ?)";
            pst = conn.prepareStatement(sql);

            final int batchSize = 1000;
            int count = 0;

            for(UserBean user : userList) {
                pst.setString(1, user.getId());
                pst.setString(2, user.getOrganId());
                pst.addBatch();

                // 一旦 batchSize 达到 1000，调用executeBatch() 提交
                if(++count % batchSize == 0) {
                    pst.executeBatch();
                }
            }

            pst.executeBatch();

        } catch (Exception e) {
            throw e;
        } finally {
            //关闭资源
            if(pst != null) {
                pst.close();
            }
            if(conn != null) {
                conn.close();
            }
        }

    }


    public boolean validateTableExist(String tableName) throws ClassNotFoundException {

        Class.forName("com.mysql.jdbc.Driver");

        boolean flag = false;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection(URL, DBConfig.username, DBConfig.password);
            DatabaseMetaData meta = conn.getMetaData();
            String type [] = {"TABLE"};

            rs = meta.getTables(null, null, tableName, type);

            flag = rs.next();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return flag;
    }


    class UserBean {

        String id;
        String username;
        String organId;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getOrganId() {
            return organId;
        }

        public void setOrganId(String organId) {
            this.organId = organId;
        }
    }
}
