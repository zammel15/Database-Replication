/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testdata;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Testdata {

    static String query_tables = "select table_name from user_tables";
    static String query_tables_columns = "select table_name, column_name, data_type, data_length from user_tab_columns where table_name=";
    static String query_data = "select * from ";
    static String query_drop_table = "drop table ";

    static void Drop(Connection cnx, String name) {
        try {
            if (cnx != null && !name.equals("")) {
                Statement st = cnx.createStatement();
                st.executeQuery(query_drop_table + name);
            }
        } catch (SQLException ex) {
            Logger.getLogger(Testdata.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void main(String[] args) throws SQLException {

        List<String> names = new ArrayList<>();
        HashMap<String, List<Column>> tables = new HashMap<String, List<Column>>();

        try {

            Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE ", "khalifa", "khalifa");
            Connection conn2 = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "khalifa2", "khalifa2");

            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query_tables);

            while (rs.next()) {
                names.add(rs.getString(1));
                System.out.println(rs.getString(1));
            }

            for (String name : names) {
                PreparedStatement pr = conn.prepareStatement(query_tables_columns + "'" + name + "'");
                rs = pr.executeQuery();
                List<Column> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(new Column(String.valueOf(rs.getObject(2)), String.valueOf(rs.getObject(3)), String.valueOf(rs.getObject(4))));
                }
                tables.put(name, rows);
            }

            for (String name : names) {
                Drop(conn2, name);
            }

            for (Map.Entry<String, List<Column>> entry : tables.entrySet()) {
                String key = entry.getKey();
                List<Column> value = entry.getValue();
                String creating = "create table " + key + "(";
                for (int i = 0; i < value.size(); i++) {
                    if (i + 1 < value.size()) {
                        creating += value.get(i).name + " " + value.get(i).type + "(" + value.get(i).length + "),";
                    } else {
                        creating += value.get(i).name + " " + value.get(i).type + "(" + value.get(i).length + ")";
                    }
                }
                creating += ")";
                Statement s2 = conn2.createStatement();
                s2.executeQuery(creating);

                System.out.println(creating);
            }

            for (String name : names) {

                st = conn.createStatement();
                rs = st.executeQuery(query_data + name);
                ResultSetMetaData md = rs.getMetaData();

                while (rs.next()) {
                    String insert = "insert into " + name + " values (";
                    for (int i = 1; i <= md.getColumnCount(); i++) {

                        if (i < md.getColumnCount()) {
                            if (md.getColumnType(i) == java.sql.Types.VARCHAR) {
                                insert += "'" + String.valueOf(rs.getObject(i)).trim() + "'" + ",";
                            } else {
                                insert += String.valueOf(rs.getObject(i)).trim() + ",";
                            }
                        } else {
                            if (md.getColumnType(i) == java.sql.Types.VARCHAR) {
                                insert += "'" + String.valueOf(rs.getObject(i)).trim() + "'";
                            } else {
                                insert += String.valueOf(rs.getObject(i)).trim();
                            }
                        }
                    }
                    insert += ")";
                    System.out.println(insert);
                    st = conn2.createStatement();
                    st.executeQuery(insert);
                }
            }

            //Drop(conn2, "etudiant");
        } catch (SQLException e) {

            System.out.println(e);
        }

    }
}
