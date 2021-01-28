package org.mine.iptable.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MysqlDBRepository implements Repository {
    private static final Logger logger = LoggerFactory.getLogger(MysqlDBRepository.class);
    private Connection connection;
    private String namespace;

    public MysqlDBRepository(String url, String username, String password, String namespace) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url, username, password);
            this.namespace = namespace;
            createTable(connection);
        } catch (Exception e) {
            logger.error("MysqlDBRepository error", e);
            throw new RuntimeException(e);
        }
    }

    private void createTable(Connection connection) {
        try {
            PreparedStatement statement = connection.prepareStatement("" +
                    "CREATE TABLE IF NOT EXISTS bigarray_repo(" +
                    "namespace VARCHAR(256) NOT NULL," +
                    "pageindex int NOT NULL," +
                    "data longblob," +
                    "PRIMARY KEY(namespace, pageindex)" +
                    ")ENGINE=InnoDB DEFAULT CHARSET=utf8;");
            statement.execute();
            statement.close();
        } catch (Exception e) {
            logger.error("create table error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int pageCount() {
        ResultSet resultSet = null;
        try (PreparedStatement statement = connection.prepareStatement("select count(1) from bigarray_repo where namespace=?")) {
            statement.setString(0, namespace);
            resultSet = statement.executeQuery();
            resultSet.next();
            return resultSet.getInt(0);
        } catch (Exception e) {
            logger.error("page count error", e);
            throw new RuntimeException(e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    logger.error("page count error", e);
                }
            }
        }
    }

    @Override
    public byte[] fetchPage(int pageIndex) {
        ResultSet resultSet = null;
        try (PreparedStatement statement = connection.prepareStatement("select data from bigarray_repo where namespace=? and pageindex=?")) {
            statement.setString(0, namespace);
            statement.setInt(1, pageIndex);
            resultSet = statement.executeQuery();
            resultSet.next();
            InputStream inputStream = resultSet.getBlob(0).getBinaryStream();
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            byte[] buf = new byte[1024 * 1024];
            int n;
            while ((n = inputStream.read(buf)) != -1) {
                byteArrayOutputStream.write(buf, 0, n);
            }
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            logger.error("fetch page error", e);
            throw new RuntimeException(e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    logger.error("fetch page error", e);
                }
            }
        }
    }

    @Override
    public boolean savePage(int pageIndex, byte[] buf) {
        ResultSet resultSet = null;
        try (PreparedStatement statement = connection.prepareStatement("replace into bigarray_repo(namespace, pageindex, data) values(?, ?, ?)")) {
            statement.setString(0, namespace);
            statement.setInt(1, pageIndex);
            statement.setBinaryStream(2, new ByteArrayInputStream(buf));
            return statement.executeUpdate() > 0;
        } catch (Exception e) {
            logger.error("fetch page error", e);
            throw new RuntimeException(e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    logger.error("fetch page error", e);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
