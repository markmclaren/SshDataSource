package com.ryanjustus.sshdatasource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import ch.ethz.ssh2.Connection;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * SshDataSource.
 *
 * BasicDataSource extended to support SSH tunnelling
 *
 * @author ryanjustus
 */
public class SshDataSource extends BasicDataSource {

  private static Log log = LogFactory.getLog(SshDataSource.class);

  private String remoteUrl = "localhost";

  private Connection sshCon;

  /**
   * Type enum. Supported database types.
   */
  public enum Type {

    MYSQL("MySql"),
    MSSQL("MsSql"),
    ORACLE("Oracle"),
    POSTGRESQL("PostgreSql");

    final String name;

    Type(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name();
    }

  }

  /**
   * Constructor. Default using ssh port 22
   *
   * @param sshUser  Ex: user@remotehost.com
   * @param password password
   */
  public SshDataSource(final String sshUser, final String password) {
    super();
    initWithPass(sshUser, 22, password);
  }

  /**
   * Constructor. Creates a BasicDataSource that uses an ssh tunnel to connect to the database
   *
   * @param sshUser  Ex: user@remotehost.com
   * @param sshPort  remote ssh server port
   * @param password ssh connection password
   */
  public SshDataSource(final String sshUser, final int sshPort, final String password) {
    super();
    initWithPass(sshUser, sshPort, password);
  }

  /**
   * Constructor. Default using ssh port 22
   *
   * @param sshUser Ex: user@remotehost.com
   * @param key     ssh keyfile in pem format
   *
   * @throws FileNotFoundException if something goes wrong
   */
  public SshDataSource(final String sshUser, final File key) throws FileNotFoundException {
    super();
    initWithKey(sshUser, 22, key);
  }

  /**
   * Constructor. Creates a BasicDataSource that uses an ssh tunnel to connect to the database
   *
   * @param sshUser formatted like 'user@remotehost'
   * @param sshPort remote ssh server port
   * @param key     ssh keyfile in pem format
   *
   * @throws FileNotFoundException if something goes wrong
   */
  public SshDataSource(final String sshUser, final int sshPort, final File key)
    throws FileNotFoundException {
    super();
    initWithKey(sshUser, sshPort, key);
  }

  /**
   * Initializes connection based on a username, password.
   *
   * @param sshUser  formatted as 'user@remotehost'
   * @param sshPort
   * @param password
   */
  private void initWithPass(final String sshUser, final int sshPort, final String password) {
    String[] user = sshUser.split("@");
    String uname = "";
    String host = "";
    if (user.length == 2) {
      uname = user[0];
      host = user[1];
    } else {
      throw new IllegalArgumentException("sshUser must be of the form user@host");
    }
    if (sshPort < 1 || sshPort > 65535) {
      throw new IllegalArgumentException("invalid ssh port");
    }
    log.info("connecting to " + host + "...");
    sshCon = new Connection(host, sshPort);
    try {
      sshCon.connect();
      sshCon.authenticateWithPassword(uname, password);
      log.info("connected");
    } catch (IOException ex) {

      log.error("Error establishing ssh connection", ex);
    }
  }

  private void initWithKey(final String sshUser, final int sshPort, final File key)
    throws FileNotFoundException {
    String[] user = sshUser.split("@");
    String uname = "";
    String host = "";
    if (user.length == 2) {
      uname = user[0];
      host = user[1];
    } else {
      throw new IllegalArgumentException("sshUser must be of the form user@host");
    }
    if (sshPort < 1 || sshPort > 65535) {
      throw new IllegalArgumentException("invalid ssh port");
    }
    try {
      sshCon = new Connection(host, sshPort);
      sshCon.authenticateWithPublicKey(uname, key, password);
    } catch (IOException ex) {
      log.error("Error establishing ssh connection", ex);
    }

  }

  @Override
  public void close() throws SQLException {
    super.close();
    sshCon.close();
  }

  /**
   * Use this instead of setUrl(String url). Makes an sql connection that is forwarded through the
   * ssh connection to the remote sql server
   *
   * @param sqlDatabaseType SshDataSource.MYSQL, SshDataSource.MSSQL, SshDataSource.ORACLE, or
   *                        SshDataSource.POSTRESQL
   * @param sqlServerPort   listening port of remote sql server
   * @param databaseName    name of the database you want to connect to or null
   */
  public void setUrl(final Type sqlDatabaseType, final int sqlServerPort,
                     final String inDatabaseName) {
    String databaseName = inDatabaseName;
    if (databaseName == null) {
      databaseName = "";
    }
    try {
      int localPort = getFreePort();

      sshCon.createLocalPortForwarder(localPort, remoteUrl, sqlServerPort);
      String sqlUrl = SshDataSource
        .getSqlConnectionPath(sqlDatabaseType, localPort, databaseName);
      super.setUrl(sqlUrl);
    } catch (IOException ex) {
      log.error("Error establishing port forwarding", ex);
    }
  }

  /**
   * setRemoteUrl. Sets the remote url you wish to connect to. This is for if the remote mysql
   * server is not located on the same computer as the remote ssh
   *
   * @param url default 'localhost'
   */
  public void setRemoteUrl(final String url) {
    this.remoteUrl = url;
  }

  /**
   * setUrl.
   *
   * @param url url
   */
  @Override
  public void setUrl(final String url) {
    throw new UnsupportedOperationException(
      "use SshDataSource.setUrl(String sqlDatabaseType, int sqlServerPort, String databaseName)");
  }

  /**
   * getSqlConnectionPath.
   *
   * @param type     sqlDatabaseType
   * @param port     port
   * @param database database
   *
   * @return SQL connection path
   */
  private static String getSqlConnectionPath(final Type type, final int port,
                                             final String database) {
    final String hostpath = "localhost";
    switch (type) {
      case MYSQL:
        return "jdbc:mysql://" + hostpath + ":" + port + "/" + database;
      case MSSQL:
        return "jdbc:jtds:sqlserver://" + hostpath + ":" + "/" + database;
      case ORACLE:
        return "jdbc:oracle:thin:@" + hostpath + ":" + port + ":" + database;
      case POSTGRESQL:
        return "jdbc:postgresql://" + hostpath + ":" + port + "/" + database;
      default:
        throw new IllegalArgumentException("unknown database type " + type);
    }
  }

  /**
   * getFreePort.
   *
   * @return free port
   */
  public static int getFreePort() {
    boolean free = false;
    int port = 0;
    while (!free) {
      port = (int) (1024 + Math.random() * (65535 - 1024));
      try {
        ServerSocket socket = new ServerSocket(port);
        socket.close();
        free = true;
      } catch (IOException ex) {
      }
    }
    return port;
  }

  /**
   * getParentLogger.
   *
   * @return Logger instance
   *
   * @throws SQLFeatureNotSupportedException if something goes wrong
   */
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

}
