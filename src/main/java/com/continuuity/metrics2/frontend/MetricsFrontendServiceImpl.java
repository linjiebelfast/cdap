package com.continuuity.metrics2.frontend;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.metrics2.common.DBUtils;
import com.continuuity.metrics2.temporaldb.DataPoint;
import com.continuuity.metrics2.temporaldb.internal.Timeseries;
import com.continuuity.metrics2.thrift.*;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import org.apache.thrift.TException;
import org.hsqldb.jdbc.pool.JDBCPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * MetricsService provides a readonly service for metrics.
 * It's a implementation that reads from the SQL supported DB.
 */
public class MetricsFrontendServiceImpl
  implements MetricsFrontendService.Iface {
  private static final Logger Log = LoggerFactory.getLogger(
    MetricsFrontendServiceImpl.class
  );

  /**
   * Connection string to connect to database.
   */
  private String connectionUrl;

  /**
   * Type of Database we are configured with.
   */
  private DBUtils.DBType type;

  /**
   *
   */
  private final ExecutorService executor
    = Executors.newCachedThreadPool();

  /**
   * DB Connection Pool manager.
   */
  private static DBConnectionPoolManager poolManager;

  public MetricsFrontendServiceImpl(CConfiguration configuration)
    throws ClassNotFoundException, SQLException {
    this.connectionUrl
      = configuration.get(Constants.CFG_METRICS_CONNECTION_URL,
                          Constants.DEFAULT_METIRCS_CONNECTION_URL);
    this.type = DBUtils.loadDriver(connectionUrl);

    // Creates a pooled data source.
    if(this.type == DBUtils.DBType.MYSQL) {
      MysqlConnectionPoolDataSource mysqlDataSource =
        new MysqlConnectionPoolDataSource();
      mysqlDataSource.setUrl(connectionUrl);
      poolManager = new DBConnectionPoolManager(mysqlDataSource, 40);
    } else if(this.type == DBUtils.DBType.HSQLDB) {
      JDBCPooledDataSource jdbcDataSource = new JDBCPooledDataSource();
      jdbcDataSource.setUrl(connectionUrl);
      poolManager = new DBConnectionPoolManager(jdbcDataSource, 40);
    }

    DBUtils.createMetricsTables(getConnection(), this.type);
  }

  /**
   * @return a {@link java.sql.Connection} based on the <code>connectionUrl</code>
   * @throws java.sql.SQLException thrown in case of any error.
   */
  private synchronized Connection getConnection() throws SQLException {
    if(poolManager != null) {
      return poolManager.getValidConnection();
    }
    return null;
  }

  /**
   * Retrieves the counters as per the {@link CounterRequest} specification.
   *
   * @param request for counters.
   * @return list of {@link Counter}
   * @throws MetricsServiceException
   * @throws TException raised when thrift related issues.
   */
  @Override
  public List<Counter> getCounters(CounterRequest request)
    throws MetricsServiceException, TException {
      List<Counter> results = Lists.newArrayList();

      // Validate all the fields passed, if any problem return an exception
      // back to client.
      validateArguments(request.getArgument());

      // If run id is passed, then use it.
      String runIdInclusion = null;
      if(request.getArgument() != null &&
        request.getArgument().isSetRunId()) {
        runIdInclusion = String.format("run_id = '%s'",
          request.getArgument().getRunId());
      }

      // If metric name list is zero, then we return all the metrics.
      StringBuffer sql = new StringBuffer();
      if(request.getName() == null || request.getName().size() == 0) {
        sql.append("SELECT flowlet_id, metric, SUM(value) AS aggr_value");
        sql.append(" ");
        sql.append("FROM metrics WHERE account_id = ? AND application_id = ?");
        sql.append(" ");
        sql.append("AND flow_id = ?");
        sql.append(" ");
        if(runIdInclusion != null) {
          sql.append("AND").append(" ").append(runIdInclusion).append(" ");
        }
        sql.append("GROUP BY flowlet_id, metric");
      } else {
        // transform the metric names by adding single quotes around
        // each metric name as they are treated as metric.
        Iterable<String> iterator =
          Iterables.transform(request.getName(), new Function<String, String>() {
            @Override
            public String apply(@Nullable String input) {
              return "'" + input + "'";
            }
          });

        // Join each with comma (,) as seperator.
        String values = Joiner.on(",").join(iterator);
        sql.append("SELECT flowlet_id, metric, SUM(value) AS aggr_value");
        sql.append(" ");
        sql.append("FROM metrics WHERE account_id = ? AND application_id = ?");
        sql.append(" ");
        sql.append("AND flow_id = ?");
        sql.append(" ");
        if(runIdInclusion != null) {
          sql.append("AND").append(" ").append(runIdInclusion).append(" ");
        }
        sql.append("metric in (").append(values).append(")").append(" ");
        sql.append("GROUP BY flowlet_id, metric");
      }

      Connection connection = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      try {
        connection = getConnection();
        stmt = connection.prepareStatement(sql.toString());
        stmt.setString(1, request.getArgument().getAccountId());
        stmt.setString(2, request.getArgument().getApplicationId());
        stmt.setString(3, request.getArgument().getFlowId());
        rs = stmt.executeQuery();
        while(rs.next()) {
          results.add(new Counter(
            rs.getString("flowlet_id"),
            rs.getString("metric"),
            rs.getFloat("aggr_value")
          ));
        }
      } catch (SQLException e) {
        Log.warn("Unable to retrieve counters. Reason : {}", e.getMessage());
      } finally {
        try {
          if(rs != null) {
            rs.close();
          }
          if(stmt != null) {
            stmt.close();
          }
          if(connection != null) {
            connection.close();
          }
        } catch(SQLException e) {
          Log.warn("Failed to close connection/statement/record. Reason : {}",
            e.getMessage());
        }
      }

      return results;
  }

  /**
   * API to request time series data for a set of metrics.
   *
   * @param request
   */
  @Override
  public Points getTimeSeries(TimeseriesRequest request)
    throws MetricsServiceException, TException {
    List<Future<List<DataPoint>>> dataPointsFuture = Lists.newArrayList();

    long start = System.currentTimeMillis()/1000;;
    long end = start;

    // Validate the timing request.
    validateTimeseriesRequest(request);

    // If start time is specified and end time is negative offset
    // from that start time, then we use that.
    if(request.isSetStartts() && request.getEndts() < 0) {
      start = request.getStartts() - request.getEndts() * 1000;
      end = request.getStartts();
    }

    // If endts is negative and the startts is not set, then we offset it
    // from the current time.
    if(! request.isSetStartts() && request.getEndts() < 0) {
      start = request.getStartts() - request.getEndts() * 1000;
    }

    // if startts is set and endts > 0 then it endts has to be greater than
    // startts.
    if(request.isSetStartts() && request.getEndts() > 0) {
      start = request.getStartts();
      end = request.getEndts();
    }

    // Preprocess the metrics list.
    List<String> preprocessedMetrics = Lists.newArrayList();
    for(String metric : request.getMetrics()) {
      if("busyness".equals(metric)) {
        preprocessedMetrics.add("tuples.read.count");
        preprocessedMetrics.add("tuples.proc.count");
      } else {
        preprocessedMetrics.add(metric);
      }
    }

    // Iterate through the metric list to be retrieved and request them
    // to be fetched in parallel.
    for(String metric : preprocessedMetrics) {
      Callable<List<DataPoint>> worker =
        new RetrieveDataPointCallable(metric, start, end, request);
      Future<List<DataPoint>> submit = executor.submit(worker);
      dataPointsFuture.add(submit);
    }

    // Now, join on all dataPoints retrieved from future.
    Map<String, List<DataPoint>> dataPoints = Maps.newHashMap();
    for(Future<List<DataPoint>> future : dataPointsFuture) {
      try {
        List<DataPoint> dataPoint = future.get();
        if(dataPoint.size() > 0) {
          String metric = dataPoint.get(0).getMetric();
          dataPoints.put(metric, dataPoint);
        }
      } catch (InterruptedException e) {
        Log.info("Timeseries retrieval has been interrupted. Reason : {}",
                 e.getMessage());
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        Log.warn("There was error getting results of a future. Reason : {}",
                 e.getMessage());
      }
    }

    Map<String, List<Point>> results = Maps.newHashMap();
    Timeseries timeseries = new Timeseries();

    // Iterate through the list of metric requested and
    for(String metric : request.getMetrics()) {
      // If the metric to be retrieved is busyness, it's a composite metric
      // and hence we retrieve the tuple.read.count and tuples.proc.count
      // and divide one by the other. This is done on the rate.
      if(metric.equals("busyness")) {
        List<DataPoint> read = dataPoints.get("tuples.read.count");
        List<DataPoint> processed = dataPoints.get("tuples.proc.count");
        if(read == null || processed == null) {
          List<DataPoint> n = null;
          results.put(metric, convertDataPointToPoint(n));
        } else {
          List<DataPoint> busyness = new Timeseries().div(
            timeseries.rate(ImmutableList.copyOf(processed)),
            timeseries.rate(ImmutableList.copyOf(read)),
            new Function<Double, Double>() {
              @Override
              public Double apply(@Nullable Double input) {
                return input * 100;
              }
            }
          );
          results.put(metric, convertDataPointToPoint(busyness));
        }
      } else {
        ImmutableList<DataPoint> r =
          new Timeseries().rate(dataPoints.get(metric));
        results.put(metric, convertDataPointToPoint(r));
      }
    }

    Points points = new Points();
    points.setPoints(results);
    return points;
  }

  /**
   * Converts List<DataPoint> to List<Point>. This is essentially done
   * to return values through thrift to frontend.
   *
   * @param points specifies a list of datapoints to be transformed to list of
   *               point.
   * @return List<Point>
   */
  List<Point> convertDataPointToPoint(List<DataPoint> points) {
    List<Point> p = Lists.newArrayList();
    if(points == null || points.size() < 1) {
      return p;
    }
    for(DataPoint point : points) {
      Point p1 = new Point();
      p1.setTimestamp(point.getTimestamp());
      p1.setValue(point.getValue());
      p.add(p1);
    }
    return p;
  }

  /**
   * Callable that's responsible for retrieving the metric requested in
   * parallel from database.
   */
  private class RetrieveDataPointCallable
    implements Callable<List<DataPoint>> {
    final String metric;
    final long start;
    final long end;
    final TimeseriesRequest request;

    public RetrieveDataPointCallable(String metric, long start, long end,
                                     TimeseriesRequest request) {
      this.metric = metric;
      this.start = start;
      this.end = end;
      this.request = request;
    }
    @Override
    public List<DataPoint> call() throws Exception {
      MetricTimeseriesLevel level = MetricTimeseriesLevel.FLOW_LEVEL;
      if(request.isSetLevel()) {
        level = request.getLevel();
      }
      return getDataPoint(metric, level, start, end, request.getArgument());
    }
  }

  /**
   * For a given metric returns a list of datapoint.
   *
   * @param metric name of metric.
   * @param level  level at which the metrics needs to be retrieved.
   * @param start  start timestamp
   * @param end    end timestamp
   * @param argument of a flow.
   * @return List<DataPoint>
   */
  List<DataPoint> getDataPoint(String metric, MetricTimeseriesLevel level,
                               long start, long end, FlowArgument argument) {
    Connection connection = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    List<DataPoint> results = new ArrayList<DataPoint>();

    try {
      // Move the window of start and end. This is to prevent the bumpyness
      // in datapoints as they are being collected from multiple sources.
      start = start - 5 ;
      end = end - 5;

      // Get the connection for database.
      connection = getConnection();

      // Generates statement for retrieving metrics at run level.
      if(level == MetricTimeseriesLevel.RUNID_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("application_id = ? AND");
        sb.append(" ").append("flow_id = ? AND");
        sb.append(" ").append("run_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric = ?");
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");

        // Connection
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, argument.getAccountId());
        stmt.setString(2, argument.getApplicationId());
        stmt.setString(3, argument.getFlowId());
        stmt.setString(4, argument.getRunId());
        stmt.setLong(5, start);
        stmt.setLong(6, end);
        stmt.setString(7, metric);
      } else if(level == MetricTimeseriesLevel.ACCOUNT_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric = ?") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, argument.getAccountId());
        stmt.setLong(2, start);
        stmt.setLong(3, end);
        stmt.setString(4, metric);
      } else if(level == MetricTimeseriesLevel.APPLICATION_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("application_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric = ?") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, argument.getAccountId());
        stmt.setString(2, argument.getApplicationId());
        stmt.setLong(3, start);
        stmt.setLong(4, end);
        stmt.setString(5, metric);
      } else if(level == MetricTimeseriesLevel.FLOW_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("application_id = ? AND");
        sb.append(" ").append("flow_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric = ?") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, argument.getAccountId());
        stmt.setString(2, argument.getApplicationId());
        stmt.setString(3, argument.getFlowId());
        stmt.setLong(4, start);
        stmt.setLong(5, end);
        stmt.setString(6, metric);
      } else if(level == MetricTimeseriesLevel.FLOWLET_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("application_id = ? AND");
        sb.append(" ").append("flow_id = ? AND");
        sb.append(" ").append("flowlet_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric = ?") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, argument.getAccountId());
        stmt.setString(2, argument.getApplicationId());
        stmt.setString(3, argument.getFlowId());
        stmt.setString(4, argument.getFlowletId());
        stmt.setLong(5, start);
        stmt.setLong(6, end);
        stmt.setString(7, metric);
      }

      // Execute the query.
      rs = stmt.executeQuery();

      // Iterate through the points.
      while(rs.next()) {
        DataPoint.Builder dpb = new DataPoint.Builder(rs.getString("metric"));
        dpb.addTimestamp(rs.getLong("timestamp"));
        dpb.addValue(rs.getFloat("aggregate"));
        results.add(dpb.create());
      }
    } catch (SQLException e) {
      Log.warn("Failed retrieving data for request {}. Reason : {}",
               argument.toString(), e.getMessage());
    } finally {
      try {
        if(rs != null) {
          rs.close();
        }
        if(stmt != null) {
          stmt.close();
        }
        if(connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        Log.warn("Failed closing recordset/statement/connection. Reason : {}",
                 e.getMessage());
      }
    }
    return results;
  }

  /**
   * @throws IllegalArgumentException thrown if issue with arguments.
   */
  private void validateArguments(FlowArgument argument)
    throws MetricsServiceException {

    // Check if there are arguments, if there are none, then we cannot
    // proceed further.
    if(argument == null) {
      throw new MetricsServiceException(
        "Arguments specifying the flow has not been provided. Please specify " +
          "account, application, flow id"
      );
    }

    if(argument.getAccountId() == null || argument.getAccountId().isEmpty()) {
      throw new MetricsServiceException("Account ID has not been specified.");
    }

    if(argument.getApplicationId() == null ||
      argument.getApplicationId().isEmpty()) {
      throw new MetricsServiceException("Application ID has not been specified");
    }

    if(argument.getFlowId() == null ||
      argument.getFlowId().isEmpty()) {
      throw new MetricsServiceException("Flow ID has not been specified.");
    }
  }

  private void validateTimeseriesRequest(TimeseriesRequest request)
    throws MetricsServiceException {

    if(! request.isSetArgument()) {
      throw new MetricsServiceException("Flow arguments should be specified.");
    }

    if(! request.isSetEndts()) {
      throw new MetricsServiceException("End time needs to be set");
    }

    if(! request.isSetMetrics()) {
      throw new MetricsServiceException("No metrics specified");
    }
  }

}
