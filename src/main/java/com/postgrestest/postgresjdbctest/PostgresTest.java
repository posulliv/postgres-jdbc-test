package com.postgrestest.postgresjdbctest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public class PostgresTest
{
    private final static String user = "starburst";
    private final static String password = "starburst";
    private final static String sql = "select sum(quantity) as sum_qt from lineitem where shipdate <= date '1998-12-01' - interval '105' day";

    public static void main(String[] args)
    {
        testCancel();
    }

    private static void testCancel()
    {
        try {
            ExecutorService executor = newDirectExecutorService();
            String sql = "SELECT quantity FROM lineitem";
            System.out.println("cancel with extended protocol");
            Connection connection = createDefaultConnection("jdbc:postgresql://localhost:5432/starburst");
            connection.setAutoCommit(false);
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setFetchSize(1000);
            long startTime = System.currentTimeMillis();
            Future<ResultSet> resultSetFuture = executor.submit(() -> {
                return statement.executeQuery();
            });
            try {
                ResultSet rs1 = resultSetFuture.get();
                int count = 0;
                while (rs1.next() && count < 10) {
                    System.out.println(rs1.getBigDecimal(1));
                    count++;
                }
                statement.cancel();
            }
            catch (ExecutionException e) {
                if (e.getCause() instanceof SQLException cause) {
                    SQLException sqlException = new SQLException(cause.getMessage(), cause.getSQLState(), cause.getErrorCode(), e);
                    if (cause.getNextException() != null) {
                        sqlException.setNextException(cause.getNextException());
                    }
                    throw sqlException;
                }
                throw new RuntimeException(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                resultSetFuture.cancel(true);
                throw new RuntimeException(e);
            }
            finally {
                long duration = System.currentTimeMillis() - startTime;
                System.out.println("took: " + duration + " ms");
            }
            statement.close();

            System.out.println("cancel with simple protocol");
            Connection simpleConnection = createSimpleConnection("jdbc:postgresql://localhost:5432/starburst");
            simpleConnection.setAutoCommit(false);
            PreparedStatement simpleStatement = simpleConnection.prepareStatement(sql);
            simpleStatement.setFetchSize(1000);
            startTime = System.currentTimeMillis();
            Future<ResultSet> resultSetFutureSimple = executor.submit(() -> {
                return simpleStatement.executeQuery();
            });
            try {
                ResultSet rs1 = resultSetFutureSimple.get();
                int count = 0;
                while (rs1.next() && count < 10) {
                    System.out.println(rs1.getBigDecimal(1));
                    count++;
                }
                statement.cancel();
            }
            catch (ExecutionException e) {
                if (e.getCause() instanceof SQLException cause) {
                    SQLException sqlException = new SQLException(cause.getMessage(), cause.getSQLState(), cause.getErrorCode(), e);
                    if (cause.getNextException() != null) {
                        sqlException.setNextException(cause.getNextException());
                    }
                    throw sqlException;
                }
                throw new RuntimeException(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                resultSetFuture.cancel(true);
                throw new RuntimeException(e);
            }
            finally {
                long duration = System.currentTimeMillis() - startTime;
                System.out.println("took: " + duration + " ms");
            }
        }
        catch (Exception e) {
            System.err.println("failure creating JDBC connection or executing query");
            e.printStackTrace();
        }
    }

    private static void testGetMetadata()
    {
        try {
            String sql = "SELECT quantity FROM lineitem";
            System.out.println("get preparedstatement metadata with extended protocol");
            Connection connection = createDefaultConnection("jdbc:postgresql://localhost:5432/starburst");
            connection.setAutoCommit(false);
            PreparedStatement statement = connection.prepareStatement(sql);
            long startTime = System.currentTimeMillis();
            ResultSetMetaData metadata = statement.getMetaData();
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            if (metadata == null) {
                System.out.println("failed to get metadata for prepared statement");
            }
            statement.close();
            System.out.println("took: " + duration + " ms");

            System.out.println("get preparedstatement metadata with simple protocol");
            Connection simpleConnection = createSimpleConnection("jdbc:postgresql://localhost:5432/starburst");
            simpleConnection.setAutoCommit(false);
            PreparedStatement simpleStatement = simpleConnection.prepareStatement(sql);
            startTime = System.currentTimeMillis();
            metadata = simpleStatement.getMetaData();
            endTime = System.currentTimeMillis();
            duration = endTime - startTime;
            if (metadata == null) {
                System.out.println("failed to get metadata for prepared statement");
            }
            simpleStatement.close();
            System.out.println("took: " + duration + " ms");
        }
        catch (Exception e) {
            System.err.println("failure creating JDBC connection or executing query");
            e.printStackTrace();
        }
    }

    private static void testSimpleQueries()
    {
        try {
            System.out.println("execute query with extended protocol and fetch size of 0");
            Connection connection = createDefaultConnection("jdbc:postgresql://localhost:5432/starburst");
            connection.setAutoCommit(false);
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setFetchSize(0);
            long startTime = System.currentTimeMillis();
            ResultSet rs1 = statement.executeQuery();
            while (rs1.next()) {
                System.out.println(rs1.getBigDecimal(1));
            }
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            rs1.close();
            statement.close();
            System.out.println("took: " + duration + " ms");

            System.out.println("execute query with extended protocol and fetch size of 1000");
            PreparedStatement fetchSizeStatement = connection.prepareStatement(sql);
            fetchSizeStatement.setFetchSize(1000);
            startTime = System.currentTimeMillis();
            ResultSet rs2 = fetchSizeStatement.executeQuery();
            while (rs2.next()) {
                System.out.println(rs2.getBigDecimal(1));
            }
            endTime = System.currentTimeMillis();
            duration = endTime - startTime;
            rs2.close();
            fetchSizeStatement.close();
            System.out.println("took: " + duration + " ms");


            System.out.println("execute query with simple query mode and fetch size of 0");
            Connection simpleConnection = createSimpleConnection("jdbc:postgresql://localhost:5432/starburst");
            simpleConnection.setAutoCommit(false);
            PreparedStatement simpleStatement = simpleConnection.prepareStatement(sql);
            simpleStatement.setFetchSize(0);
            startTime = System.currentTimeMillis();
            ResultSet rs3 = simpleStatement.executeQuery();
            while (rs3.next()) {
                System.out.println(rs3.getBigDecimal(1));
            }
            endTime = System.currentTimeMillis();
            duration = endTime - startTime;
            rs3.close();
            simpleStatement.close();
            System.out.println("took: " + duration + " ms");

            System.out.println("execute query with simple query mode and fetch size of 1000");
            PreparedStatement simpleFetchSizeStatement = simpleConnection.prepareStatement(sql);
            simpleFetchSizeStatement.setFetchSize(1000);
            startTime = System.currentTimeMillis();
            ResultSet rs4 = simpleFetchSizeStatement.executeQuery();
            while (rs4.next()) {
                System.out.println(rs4.getBigDecimal(1));
            }
            endTime = System.currentTimeMillis();
            duration = endTime - startTime;
            rs4.close();
            simpleFetchSizeStatement.close();
            System.out.println("took: " + duration + " ms");
        }
        catch (Exception e) {
            System.err.println("failure creating JDBC connection or executing query");
            e.printStackTrace();
        }
    }

    private static Connection createDefaultConnection(String jdbcUrl)
            throws SQLException, IOException
    {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        return DriverManager.getConnection(jdbcUrl, properties);
    }

    private static Connection createSimpleConnection(String jdbcUrl)
            throws SQLException, IOException
    {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("preferQueryMode", "simple");
        return DriverManager.getConnection(jdbcUrl, properties);
    }
}