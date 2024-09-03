package de.cwlounge.coresystem.bansystem.Database;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseManager {

    private ConcurrentHashMap<Object, Long> garbage = new ConcurrentHashMap<Object, Long>();

    private boolean garbagecollect = false;

    private boolean garbagedebug = false;

    private int cleartime = 1000;

    private String port = "3306";
    private String host;
    private String database;
    private String user;
    private String password;
    private boolean autoreconnect = true;
    private boolean async = false;
    private Connection connection;

    public DatabaseManager(String host, String user, String database, String password) {
        this.setHost(host);
        this.setUser(user);
        this.setDatabase(database);
        this.setPassword(password);
    }

    public DatabaseManager(String host, String port, String user, String database, String password) {
        this.setHost(host);
        this.setPort(port);
        this.setUser(user);
        this.setDatabase(database);
        this.setPassword(password);
    }

    public DatabaseManager Connect() {
        return Open();
    }

    public DatabaseManager Open() {
        run(() -> {
            try {
                String extras = "&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
                String login = "jdbc:mysql://" + host + ":" + port + "/" + database + "?characterEncoding=utf8"
                        + extras;
                connection = DriverManager.getConnection(login, user, password);
            } catch (SQLException e) {
                System.out.println("Datenbankverbindung Abgebrochen: " + e.getSQLState());
                e.printStackTrace();
            }
            if (garbagecollect) {
                startasync(() -> {
                    try {
                        while (!connection.isClosed()) {
                            for (Map.Entry<Object, Long> entry : garbage.entrySet()) {
                                if (entry.getKey() != null) {
                                    if (entry.getValue() < (System.currentTimeMillis() - cleartime)) {
                                        Object object = entry.getKey();
                                        try {
                                            if (object instanceof Statement) {
                                                Statement st = (Statement) object;
                                                if (!st.isClosed()) {
                                                    if (garbagedebug) {
                                                        System.out.println("Unclosed Statement Found");
                                                    }
                                                    st.close();
                                                }
                                                garbage.remove(entry.getKey(), entry.getValue());
                                            } else if (object instanceof ResultSet) {
                                                ResultSet rs = (ResultSet) object;
                                                if (!rs.isClosed()) {
                                                    if (garbagedebug) {
                                                        System.out.println("Unclosed Resultset Found");
                                                    }
                                                    rs.close();
                                                }
                                                garbage.remove(entry.getKey(), entry.getValue());
                                            } else if (object instanceof PreparedStatement) {
                                                PreparedStatement st = (PreparedStatement) object;
                                                if (!st.isClosed()) {
                                                    if (garbagedebug) {
                                                        System.out.println("Unclosed PreparedStatement Found");
                                                    }
                                                    st.close();
                                                }
                                                garbage.remove(entry.getKey(), entry.getValue());
                                            } else if (object instanceof AutoCloseable) {
                                                AutoCloseable st = (AutoCloseable) object;
                                                if (garbagedebug) {
                                                    System.out.println("Unclosed AutoCloseable Found");
                                                }
                                                st.close();
                                                garbage.remove(entry.getKey(), entry.getValue());
                                            }
                                        } catch (Exception e) {
                                            garbage.remove(entry.getKey(), entry.getValue());
                                            e.printStackTrace();
                                        }
                                    }
                                } else {
                                    garbage.remove(entry.getKey(), entry.getValue());
                                }
                            }
                            try {
                                Thread.sleep(cleartime);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            }
        });
        return this;
    }

    public DatabaseManager Disconnect() {
        return Close();
    }

    public int garbagesize() {
        return garbage.size();
    }

    public DatabaseManager Close() {
        run(() -> {
            try {
                connection.close();
            } catch (SQLException e) {
                System.out.println("Datenbankverbindung Abgebrochen: " + e.getSQLState());
            }
        });
        return this;
    }

    public DatabaseManager run(Runnable run) {
        if (async) {
            startasync(run);
        } else {
            run.run();
        }
        return this;
    }

    public void startasync(Runnable run) {
        new Thread(() -> {
            try {
                run.run();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }).start();

    }

    public String getHost() {
        return host;
    }

    public DatabaseManager setHost(String host) {
        this.host = host;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public DatabaseManager setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getUser() {
        return user;
    }

    public DatabaseManager setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public DatabaseManager setPassword(String password) {
        this.password = password;
        return this;
    }

    public Connection getConnection() {
        return connection;
    }

    public boolean isAutoreconnect() {
        return autoreconnect;
    }

    public boolean isConnected() {
        return connection != null;
    }

    public DatabaseManager setAutoreconnect(boolean autoreconnect) {
        this.autoreconnect = autoreconnect;
        return this;
    }

    public String getPort() {
        return port;
    }

    public DatabaseManager setPort(String port) {
        this.port = port;
        return this;
    }

    public PreparedStatement getStatement(String sql) {
        if (isConnected()) {
            try {
                PreparedStatement pst = connection.prepareStatement(sql);
                if (garbagecollect) {
                    garbage.put(pst, System.currentTimeMillis());
                }
                return pst;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public ResultSet query(final String qry) {
        if (isConnected()) {
            try {
                Statement st = connection.createStatement();
                st.executeQuery(qry);
                ResultSet resultset = st.getResultSet();
                if (garbagecollect) {
                    garbage.put(st, System.currentTimeMillis());
                    garbage.put(resultset, System.currentTimeMillis());
                } else {
                }
                return resultset;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public DatabaseManager update(String qry) {
        if (isConnected()) {
            run(() -> {
                try {
                    Statement st = connection.createStatement();
                    if (garbagecollect) {
                        garbage.put(st, System.currentTimeMillis());
                    }
                    st.executeUpdate(qry);
                    st.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }
        return this;
    }

    public DatabaseManager createTable(String tablename, String values) {
        run(() -> {
            try {
                PreparedStatement ps = getStatement("CREATE TABLE IF NOT EXISTS " + tablename + " (" + values + ")");
                ps.executeUpdate();
                ps.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        return this;
    }

    public DatabaseManager insert(String table, String tablevalues, String values) {
        run(() -> {
            update("INSERT INTO " + table + "(" + tablevalues + ") VALUES (" + values + ");");
        });
        return this;
    }

    public DatabaseManager delete(String table, String uuidname, String uuid, String uuidname2, String uuid2) {
        run(() -> {
            update("DELETE FROM `" + table + "` WHERE `" + uuidname + "`='" + uuid + "' AND `" + uuidname2 + "`='"
                    + uuid2 + "'");
        });
        return this;
    }

    public DatabaseManager delete(String table, String uuidname, String uuid) {
        run(() -> {
            update("DELETE FROM `" + table + "` WHERE `" + uuidname + "`='" + uuid + "'");
        });
        return this;
    }

    public boolean dataexist(String table, String uuidname, String uuid) {
        try {
            String data = null;
            ResultSet rs = query("SELECT * FROM " + table + " WHERE " + uuidname + "='" + uuid + "'");
            if (rs.next()) {
                data = rs.getString(uuidname);
            }
            rs.getStatement().close();
            rs.close();
            return data != null;
        } catch (SQLException e) {
        }

        return false;
    }


    public ArrayList<String> getStringListBySortierung(String table, String getvalue, String sorting, int i) {
        ResultSet rs = query("SELECT " + getvalue + " FROM " + table + " ORDER BY " + sorting + " DESC LIMIT " + i);
        ArrayList<String> list = new ArrayList<String>();
        if (rs == null) {
            return list;
        }
        try {
            while (rs.next()) {
                list.add(rs.getString(getvalue));
            }
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public ArrayList<Integer> getIntListBySortierung(String table, String getvalue, String sorting, int i) {
        ResultSet rs = query("SELECT " + getvalue + " FROM " + table + " ORDER BY " + sorting + " DESC LIMIT " + i);
        ArrayList<Integer> list = new ArrayList<Integer>();
        if (rs == null) {
            return list;
        }
        try {
            while (rs.next()) {
                list.add(rs.getInt(getvalue));
            }
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public ArrayList<Long> getLongListBySortierung(String table, String getvalue, String sorting, int i) {
        ResultSet rs = query("SELECT " + getvalue + " FROM " + table + " ORDER BY " + sorting + " DESC LIMIT " + i);
        ArrayList<Long> list = new ArrayList<Long>();
        if (rs == null) {
            return list;
        }
        try {
            while (rs.next()) {
                list.add(rs.getLong(getvalue));
            }
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public ArrayList<String> getStringList(String table, String getvalue, String sorting) {
        ResultSet rs = query("SELECT " + getvalue + " FROM " + table + " ORDER BY " + sorting + " DESC");
        ArrayList<String> list = new ArrayList<String>();
        if (rs == null) {
            return list;
        }
        try {
            while (rs.next()) {
                list.add(rs.getString(getvalue));
            }
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }


    public ArrayList<String> getStringList(String table, String getvalue, String sorting, String where, String wherevalue) {
        ResultSet rs = query("SELECT " + getvalue + " FROM " + table + " WHERE "+where+"='"+wherevalue+"' ORDER BY " + sorting + " DESC");
        ArrayList<String> list = new ArrayList<String>();
        if (rs == null) {
            return list;
        }
        try {
            while (rs.next()) {
                list.add(rs.getString(getvalue));
            }
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public ArrayList<String> getStringList(String table, String getvalue) {
        ResultSet rs = query("SELECT " + getvalue + " FROM " + table + "");
        ArrayList<String> list = new ArrayList<String>();
        if (rs == null) {
            return list;
        }
        try {
            while (rs.next()) {
                list.add(rs.getString(getvalue));
            }
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public ArrayList<Integer> getIntList(String table, String getvalue, String sorting) {
        ResultSet rs = query("SELECT " + getvalue + " FROM " + table + " ORDER BY " + sorting);
        ArrayList<Integer> list = new ArrayList<Integer>();
        if (rs == null) {
            return list;
        }
        try {
            while (rs.next()) {
                list.add(rs.getInt(getvalue));
            }
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public ArrayList<Integer> getIntList(String table, String getvalue, String sorting,String whereuuid, String where) {
        ResultSet rs = query("SELECT " + getvalue + " FROM " + table + " WHERE "+whereuuid+"='"+where+"' ORDER BY " + sorting);
        ArrayList<Integer> list = new ArrayList<Integer>();
        if (rs == null) {
            return list;
        }
        try {
            while (rs.next()) {
                try {
                    list.add(rs.getInt(getvalue));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public ArrayList<Long> getLongList(String table, String getvalue, String sorting) {
        ResultSet rs = query("SELECT " + getvalue + " FROM " + table + " ORDER BY " + sorting);
        ArrayList<Long> list = new ArrayList<Long>();
        if (rs == null) {
            return list;
        }
        try {
            while (rs.next()) {
                list.add(rs.getLong(getvalue));
            }
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public int getInt(String table, String uuidname, String uuid, String returnvalue) {
        int i = 0;
        try {
            ResultSet rs = query(
                    "SELECT " + returnvalue + " FROM " + table + " WHERE " + uuidname + "= '" + uuid + "'");
            if (rs.next()) {
                rs.getInt(returnvalue);
            }
            i = rs.getInt(returnvalue);
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }

    public BigInteger getBigInt(String table, String uuidname, String uuid, String returnvalue) {
        BigInteger i = BigInteger.valueOf(0);
        try {
            ResultSet rs = query(
                    "SELECT " + returnvalue + " FROM " + table + " WHERE " + uuidname + "= '" + uuid + "'");
            if (rs.next()) {
                rs.getLong(returnvalue);
            }
            i = BigInteger.valueOf(rs.getLong(returnvalue));
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }

    public Long getLong(String table, String uuidname, String uuid, String returnvalue) {
        Long i = 0L;
        try {
            ResultSet rs = query(
                    "SELECT " + returnvalue + " FROM " + table + " WHERE " + uuidname + "= '" + uuid + "'");
            if (rs.next()) {
                rs.getLong(returnvalue);
            }
            i = rs.getLong(returnvalue);
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }

    public String getString(String table, String uuidname, String uuid, String returnvalue) {
        String i = "";
        try {
            ResultSet rs = query(
                    "SELECT " + returnvalue + " FROM " + table + " WHERE " + uuidname + "= '" + uuid + "'");
            if (rs.next()) {
                rs.getString(returnvalue);
            }
            i = rs.getString(returnvalue);
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }


    public DatabaseManager setInt(String table, String uuidname, String uuid, String targetedvalue, Integer i) {
        run(() -> {
            update("UPDATE " + table + " SET " + targetedvalue + "= '" + i + "'WHERE " + uuidname + "= '" + uuid
                    + "';");
        });
        return this;
    }

    public DatabaseManager setBigInt(String table, String uuidname, String uuid, String targetedvalue, BigInteger i) {
        run(() -> {
            update("UPDATE " + table + " SET " + targetedvalue + "= '" + i + "'WHERE " + uuidname + "= '" + uuid
                    + "';");
        });
        return this;
    }

    public DatabaseManager setLong(String table, String uuidname, String uuid, String targetedvalue, Long i) {
        run(() -> {
            update("UPDATE " + table + " SET " + targetedvalue + "= '" + BigInteger.valueOf(i) + "'WHERE " + uuidname
                    + "= '" + uuid + "';");
        });
        return this;
    }

    public DatabaseManager setString(String table, String uuidname, String uuid, String targetedvalue, String i) {
        run(() -> {
            update("UPDATE " + table + " SET " + targetedvalue + "= '" + i + "'WHERE " + uuidname + "= '" + uuid + "';");
        });
        return this;
    }
    public boolean isAsync() {
        return async;
    }

    public DatabaseManager setAsync(boolean async) {
        this.async = async;
        return this;
    }

    public boolean isGarbagecollect() {
        return garbagecollect;
    }

    public void setGarbagecollect(boolean garbagecollect) {
        this.garbagecollect = garbagecollect;
    }

    public int getCleartime() {
        return cleartime;
    }

    public void setCleartime(int cleartime) {
        this.cleartime = cleartime;
    }

    public boolean isGarbagedebug() {
        return garbagedebug;
    }

    public void setGarbagedebug(boolean garbagedebug) {
        this.garbagedebug = garbagedebug;
    }







    public boolean pdataexist(String table, String uuidname, String uuid) {
        try {
            String data = null;
            ResultSet rs = ParameterizedQuery("SELECT * FROM " + table + " WHERE " + uuidname + "=" + "?" + "",uuid);
            if (rs.next()) {
                data = rs.getString(uuidname);
            }
            rs.getStatement().close();
            rs.close();
            return data != null;
        } catch (SQLException e) {
        }

        return false;
    }


    public ResultSet ParameterizedQuery(String qry, String... values) {
        if (isConnected()) {
            try {
                PreparedStatement prepst = connection.prepareStatement(qry);
                int i = 1;
                for (String value : values) {
                    prepst.setString(i++, value);
                }
                ResultSet rs = prepst.executeQuery();
                if (garbagecollect) {
                    garbage.put(prepst, System.currentTimeMillis());
                    garbage.put(rs, System.currentTimeMillis());
                }
                return rs;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public DatabaseManager pupdate(String qry,String... values) {
        if (isConnected()) {
            run(() -> {
                try {
                    PreparedStatement st = connection.prepareStatement(qry);
                    int i = 1;
                    for (String value : values) {
                        st.setString(i++, value);
                    }
                    if (garbagecollect) {
                        garbage.put(st, System.currentTimeMillis());
                    }
                    st.executeUpdate();
                    st.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }
        return this;
    }

    public DatabaseManager pinsert(String table, String tablevalues, String... values) {
        run(() -> {
            String s = "?";
            for (int i = 1; i != values.length ;i++) {
                s += ", " + "?";
            }
            pupdate("INSERT INTO " + table + "(" + tablevalues + ") VALUES (" + s + ");",values);
        });
        return this;
    }

    public DatabaseManager psetString(String table, String uuidname, String uuid, String targetedvalue, String i) {
        run(() -> {
            pupdate("UPDATE " + table + " SET " + targetedvalue + "= " + "?" + " WHERE " + uuidname + "= " + "?" + " ;",i,uuid);
        });
        return this;
    }

    public DatabaseManager pdelete(String table, String uuidname, String uuid) {
        run(() -> {
            pupdate("DELETE FROM `" + table + "` WHERE `" + uuidname + "`=" + "?",uuid);
        });
        return this;
    }


    public String pgetString(String table, String uuidname, String uuid, String returnvalue) {
        String i = "";
        try {
            ResultSet rs = ParameterizedQuery("SELECT " + returnvalue + " FROM " + table + " WHERE " + uuidname + "= " + "?" + "",uuid);
            if (rs.next()) {
                rs.getString(returnvalue);
            }
            i = rs.getString(returnvalue);
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }




}
