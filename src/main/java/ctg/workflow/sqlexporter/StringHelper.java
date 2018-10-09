package ctg.workflow.sqlexporter;

public class StringHelper
{
    public static String getCountSql(String sql)
    {
        String nsql = "";
        if (sql != null) {
            sql = sql.replace(" FROM ", " from ").replace(" UNION ", " union ").replace(" JOIN ", " join ").replace("SELECT ", "select ");
        }
        Boolean isjoin = Boolean.valueOf(false);
        String xsql = sql;
        if ((sql.indexOf(" union ") != -1) || (sql.toLowerCase().indexOf(" max(") != -1) || (sql.toLowerCase().indexOf(" min(") != -1) || (sql.toLowerCase().indexOf(" avg(") != -1) || (sql.toLowerCase().indexOf(" sum(") != -1) || ((sql.indexOf(" from ") != -1) && (sql.split(" from ").length > 3))) {
            return "select count(1) from (" + sql + ") AAAA";
        }
        if (sql.indexOf(" join ") != -1)
        {
            isjoin = Boolean.valueOf(true);
            sql = sql.split(" join ")[0];
        }
        if (sql.indexOf(" from ") != -1)
        {
            String[] arr = sql.split(" from ");
            if (arr.length > 2)
            {
                String ssql = "";
                if (arr[0].split("select ").length == 2)
                {
                    for (int j = 1; j < arr.length; j++) {
                        ssql = ssql + " from " + arr[j];
                    }
                    nsql = "select count(1) " + ssql;
                }
                else
                {
                    for (int i = 0; i < arr.length; i++)
                    {
                        String[] arr2 = arr[i].split("select ");
                        if (arr2.length < 2)
                        {
                            for (int j = i + 1; j < arr.length; j++) {
                                ssql = ssql + " from " + arr[j];
                            }
                            nsql = "select count(1) " + ssql;
                        }
                    }
                }
            }
            else
            {
                nsql = "select count(1) " + sql.substring(sql.indexOf(" from "));
            }
        }
        if ((isjoin.booleanValue()) && (!xsql.isEmpty())) {
            for (int x = 1; x < xsql.split(" join ").length; x++) {
                nsql = nsql + " join " + xsql.split(" join ")[x];
            }
        }
        return nsql;
    }

    public static void main(String[] args)
    {
        String sqlString = "SELECT a,b,c,(SELECT d from b) as dd,(select y from b) as ddd,(select y from b) as ddd from aaa where x=y and n in (select y from b)";
        System.out.println(getCountSql(sqlString));
        System.out.println(getCountSql("select a,b from xx"));
        System.out.println(getCountSql("select a,b from xx union select c,d from yy"));
        System.out.println(getCountSql("select a,b from xx where x=y and n in (select y from b)"));
        System.out.println(getCountSql("select a1,(select ac from tt1 where x=1) a2 from t1 left join (select a2,(select a3 from tt3) x from tt2) a3 where x=y and n in (select y from b)"));
    }
}
