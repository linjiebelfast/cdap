/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.app.runtime.distributed.launcher;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.launcher.LaunchInfo;
import io.cdap.cdap.runtime.spi.launcher.Launcher;
import io.cdap.cdap.runtime.spi.launcher.LauncherFile;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import joptsimple.OptionSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.Arguments;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.DefaultRuntimeSpecification;
import org.apache.twill.internal.DefaultTwillSpecification;
import org.apache.twill.internal.JvmOptions;
import org.apache.twill.internal.LogOnlyEventHandler;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.appmaster.ApplicationMasterMain;
import org.apache.twill.internal.container.TwillContainerMain;
import org.apache.twill.internal.io.LocationCache;
import org.apache.twill.internal.json.ArgumentsCodec;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.apache.twill.internal.utils.Dependencies;
import org.apache.twill.internal.utils.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * launch dataproc job with main class to launch yarn job.
 */
public class LauncherTwillPreparer implements TwillPreparer {
  private static final Logger LOG = LoggerFactory.getLogger(LauncherTwillPreparer.class);
  private static final Gson GSON = new Gson();

  private static final String SETUP_SPARK_SH = "setupSpark.sh";
  private static final String SETUP_SPARK_PY = "setupSpark.py";
  private static final String SPARK_ENV_SH = "sparkEnv.sh";

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final TwillSpecification twillSpec;
  private final ProgramRunId programRunId;
  private final ProgramOptions programOptions;

  private final List<String> arguments = new ArrayList<>();
  private final Set<Class<?>> dependencies = Sets.newIdentityHashSet();
  private final List<URI> resources = new ArrayList<>();
  private final List<String> classPaths = new ArrayList<>();
  private final ListMultimap<String, String> runnableArgs = ArrayListMultimap.create();
  private final Map<String, Map<String, String>> environments = new HashMap<>();
  private final List<String> applicationClassPaths = new ArrayList<>();
  //private final Credentials credentials;
  private final Map<String, Map<String, String>> logLevels = new HashMap<>();
  private final LocationCache locationCache;
  private final Map<String, Integer> maxRetries = new HashMap<>();
  private final Map<String, Map<String, String>> runnableConfigs = new HashMap<>();
  private final Map<String, String> runnableExtraOptions = new HashMap<>();
  private final LocationFactory locationFactory;
  private final Launcher launcher;

  private String extraOptions;
  private JvmOptions.DebugOptions debugOptions;
  private ClassAcceptor classAcceptor;
  private String classLoaderClassName;

  LauncherTwillPreparer(CConfiguration cConf, Configuration hConf,
                        TwillSpecification twillSpec, ProgramRunId programRunId, ProgramOptions programOptions,
                        @Nullable String extraOptions,
                        LocationCache locationCache, LocationFactory locationFactory, Launcher launcher) {
    // Check to prevent future mistake
    if (twillSpec.getRunnables().size() != 1) {
      throw new IllegalArgumentException("Only one TwillRunnable is supported");
    }

    this.debugOptions = JvmOptions.DebugOptions.NO_DEBUG;
    this.cConf = cConf;
    this.hConf = hConf;
    this.twillSpec = twillSpec;
    this.programRunId = programRunId;
    this.programOptions = programOptions;
    this.extraOptions = extraOptions == null ? "" : extraOptions;
    this.classAcceptor = new ClassAcceptor();
    this.locationCache = locationCache;
    this.locationFactory = locationFactory;
    this.extraOptions = cConf.get(io.cdap.cdap.common.conf.Constants.AppFabric.PROGRAM_JVM_OPTS);
    this.launcher = launcher;
  }

  private void confirmRunnableName(String runnableName) {
    Preconditions.checkNotNull(runnableName);
    Preconditions.checkArgument(twillSpec.getRunnables().containsKey(runnableName),
                                "Runnable %s is not defined in the application.", runnableName);
  }

  @Override
  public TwillPreparer withConfiguration(Map<String, String> config) {
    config.forEach(hConf::set);
    return this;
  }

  @Override
  public TwillPreparer withConfiguration(String runnableName, Map<String, String> config) {
    confirmRunnableName(runnableName);
    runnableConfigs.put(runnableName, Maps.newHashMap(config));
    return this;
  }

  @Override
  public TwillPreparer addLogHandler(LogHandler handler) {
    LOG.trace("LogHandler is not supported for {}", getClass().getSimpleName());
    return this;
  }

  @Override
  public TwillPreparer setUser(String user) {
    return this;
  }

  @Override
  public TwillPreparer setSchedulerQueue(String name) {
    LOG.trace("Scheduler queue is not supported for {}", getClass().getSimpleName());
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String options) {
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    this.extraOptions = options;
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String runnableName, String options) {
    confirmRunnableName(runnableName);
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    runnableExtraOptions.put(runnableName, options);
    return this;
  }

  @Override
  public TwillPreparer addJVMOptions(String options) {
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    this.extraOptions = extraOptions.isEmpty() ? options : extraOptions + " " + options;
    return this;
  }

  @Override
  public TwillPreparer enableDebugging(String... runnables) {
    return enableDebugging(false, runnables);
  }

  @Override
  public TwillPreparer enableDebugging(boolean doSuspend, String... runnables) {
    List<String> runnableList = Arrays.asList(runnables);
    runnableList.forEach(this::confirmRunnableName);
    this.debugOptions = new JvmOptions.DebugOptions(true, doSuspend, runnableList);
    return this;
  }

  @Override
  public TwillPreparer withApplicationArguments(String... args) {
    return withApplicationArguments(Arrays.asList(args));
  }

  @Override
  public TwillPreparer withApplicationArguments(Iterable<String> args) {
    Iterables.addAll(arguments, args);
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    return withArguments(runnableName, Arrays.asList(args));
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    confirmRunnableName(runnableName);
    runnableArgs.putAll(runnableName, args);
    return this;
  }

  @Override
  public TwillPreparer withDependencies(Class<?>... classes) {
    return withDependencies(Arrays.asList(classes));
  }

  @Override
  public TwillPreparer withDependencies(Iterable<Class<?>> classes) {
    Iterables.addAll(dependencies, classes);
    return this;
  }

  @Override
  public TwillPreparer withResources(URI... resources) {
    return withResources(Arrays.asList(resources));
  }

  @Override
  public TwillPreparer withResources(Iterable<URI> resources) {
    Iterables.addAll(this.resources, resources);
    return this;
  }

  @Override
  public TwillPreparer withClassPaths(String... classPaths) {
    return withClassPaths(Arrays.asList(classPaths));
  }

  @Override
  public TwillPreparer withClassPaths(Iterable<String> classPaths) {
    Iterables.addAll(this.classPaths, classPaths);
    return this;
  }

  @Override
  public TwillPreparer withEnv(Map<String, String> env) {
    // Add the given environments to all runnables
    for (String runnableName : twillSpec.getRunnables().keySet()) {
      setEnv(runnableName, env, false);
    }
    return this;
  }

  @Override
  public TwillPreparer withEnv(String runnableName, Map<String, String> env) {
    confirmRunnableName(runnableName);
    setEnv(runnableName, env, true);
    return this;
  }

  @Override
  public TwillPreparer withApplicationClassPaths(String... classPaths) {
    return withApplicationClassPaths(Arrays.asList(classPaths));
  }

  @Override
  public TwillPreparer withApplicationClassPaths(Iterable<String> classPaths) {
    Iterables.addAll(this.applicationClassPaths, classPaths);
    return this;
  }

  @Override
  public TwillPreparer withBundlerClassAcceptor(ClassAcceptor classAcceptor) {
    this.classAcceptor = classAcceptor;
    return this;
  }

  @Override
  public TwillPreparer withMaxRetries(String runnableName, int maxRetries) {
    confirmRunnableName(runnableName);
    this.maxRetries.put(runnableName, maxRetries);
    return this;
  }

  @Override
  public TwillPreparer addSecureStore(SecureStore secureStore) {
    return this;
  }

  @Override
  public TwillPreparer setLogLevel(LogEntry.Level logLevel) {
    return setLogLevels(Collections.singletonMap(Logger.ROOT_LOGGER_NAME, logLevel));
  }

  @Override
  public TwillPreparer setLogLevels(Map<String, LogEntry.Level> logLevels) {
    Preconditions.checkNotNull(logLevels);
    for (String runnableName : twillSpec.getRunnables().keySet()) {
      saveLogLevels(runnableName, logLevels);
    }
    return this;
  }

  @Override
  public TwillPreparer setLogLevels(String runnableName, Map<String, LogEntry.Level> runnableLogLevels) {
    confirmRunnableName(runnableName);
    Preconditions.checkNotNull(runnableLogLevels);
    Preconditions.checkArgument(!(logLevels.containsKey(Logger.ROOT_LOGGER_NAME)
      && logLevels.get(Logger.ROOT_LOGGER_NAME) == null));
    saveLogLevels(runnableName, runnableLogLevels);
    return this;
  }

  @Override
  public TwillPreparer setClassLoader(String classLoaderClassName) {
    this.classLoaderClassName = classLoaderClassName;
    return this;
  }

  @Override
  public TwillController start() {
    return start(Constants.APPLICATION_MAX_START_SECONDS, TimeUnit.SECONDS);
  }

  @Override
  public TwillController start(long timeout, TimeUnit timeoutUnit) {
    long startTime = System.currentTimeMillis();

    try {
      Path tempDir = java.nio.file.Paths.get(
        cConf.get(io.cdap.cdap.common.conf.Constants.CFG_LOCAL_DATA_DIR),
        cConf.get(io.cdap.cdap.common.conf.Constants.AppFabric.TEMP_DIR)).toAbsolutePath();
      Path stagingDir = Files.createTempDirectory(tempDir, programRunId.getRun());

      LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(
        programRunId, programOptions.getArguments().asMap());
      Cancellable cancelLoggingContext = LoggingContextAccessor.setLoggingContext(loggingContext);

      Map<String, LocalFile> localFiles = new HashMap<>();

      createLauncherJar(localFiles);
      createTwillJar(createBundler(classAcceptor, stagingDir), localFiles);
      createApplicationJar(createBundler(classAcceptor, stagingDir), localFiles);
      createResourcesJar(createBundler(classAcceptor, stagingDir), localFiles, stagingDir);

      throwIfTimeout(startTime, timeout, timeoutUnit);

      TwillRuntimeSpecification twillRuntimeSpec;
      Path runtimeConfigDir = Files.createTempDirectory(stagingDir, Constants.Files.RUNTIME_CONFIG_JAR);
      twillRuntimeSpec = saveSpecification(twillSpec,
                                           runtimeConfigDir.resolve(Constants.Files.TWILL_SPEC), stagingDir, launcher);
      RuntimeSpecification runtimeSpec = twillRuntimeSpec.getTwillSpecification().getRunnables().values()
        .stream().findFirst().orElseThrow(IllegalStateException::new);
      saveLogback(runtimeConfigDir.resolve(Constants.Files.LOGBACK_TEMPLATE));
      saveClassPaths(runtimeConfigDir);
      saveArguments(new Arguments(arguments, runnableArgs), runtimeConfigDir.resolve(Constants.Files.ARGUMENTS));
      saveResource(runtimeConfigDir, SETUP_SPARK_SH);
      saveResource(runtimeConfigDir, SETUP_SPARK_PY);
      createRuntimeConfigJar(runtimeConfigDir, localFiles, stagingDir);
      Paths.deleteRecursively(runtimeConfigDir);

      throwIfTimeout(startTime, timeout, timeoutUnit);
      List<LauncherFile> launcherFiles = new ArrayList<>();
      for (Map.Entry<String, LocalFile> entry : localFiles.entrySet()) {
        LauncherFile launcherFile = new LauncherFile(entry.getKey(), entry.getValue().getURI(),
                                                      entry.getValue().isArchive());
        LOG.info("### Adding file : {}", launcherFile);
        launcherFiles.add(launcherFile);
      }
      for (LocalFile file : runtimeSpec.getLocalFiles()) {
        LauncherFile launcherFile = new LauncherFile(file.getName(), file.getURI(), file.isArchive());
        launcherFiles.add(launcherFile);
        LOG.info("### Adding file : {}", launcherFile);
      }

      LOG.info("####### Finally launching job");

      Cluster cluster = GSON.fromJson(programOptions.getArguments().getOption(ProgramOptionConstants.CLUSTER),
                                      Cluster.class);
      LaunchInfo info = new LaunchInfo(programRunId.getRun(), cluster.getName(), launcherFiles,
                                       cluster.getProperties());
      launcher.launch(info);
      cancelLoggingContext.cancel();
      DirUtils.deleteDirectoryContents(stagingDir.toFile(), false);
    } catch (Exception e) {
      LOG.error("Exception while starting the preparer and creating controller. {}", e.getMessage(), e);
    }

    return new LauncherTwillConrtoller(RunIds.fromString(programRunId.getRun()));
  }

  /**
   * Throws a {@link TimeoutException} if time passed since the given start time has exceeded the given timeout value.
   */
  private void throwIfTimeout(long startTime, long timeout, TimeUnit timeoutUnit) throws TimeoutException {
    long timeoutMillis = timeoutUnit.toMillis(timeout);
    if (System.currentTimeMillis() - startTime >= timeoutMillis) {
      throw new TimeoutException(String.format("Aborting startup of program run %s due to timeout after %d %s",
                                               programRunId, timeout, timeoutUnit.name().toLowerCase()));
    }
  }

  /**
   * Returns the extra options for the container JVM.
   */
  private String addClassLoaderClassName(String extraOptions) {
    if (classLoaderClassName == null) {
      return extraOptions;
    }
    String classLoaderProperty = "-D" + Constants.TWILL_CONTAINER_CLASSLOADER + "=" + classLoaderClassName;
    return extraOptions.isEmpty() ? classLoaderProperty : extraOptions + " " + classLoaderProperty;
  }

  private void setEnv(String runnableName, Map<String, String> env, boolean overwrite) {
    Map<String, String> environment = environments.get(runnableName);
    if (environment == null) {
      environment = new LinkedHashMap<>(env);
      environments.put(runnableName, environment);
      return;
    }

    for (Map.Entry<String, String> entry : env.entrySet()) {
      if (overwrite || !environment.containsKey(entry.getKey())) {
        environment.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private void saveLogLevels(String runnableName, Map<String, LogEntry.Level> logLevels) {
    Map<String, String> newLevels = new HashMap<>();
    for (Map.Entry<String, LogEntry.Level> entry : logLevels.entrySet()) {
      Preconditions.checkArgument(entry.getValue() != null,
                                  "Log level cannot be null for logger {}", entry.getKey());
      newLevels.put(entry.getKey(), entry.getValue().name());
    }
    this.logLevels.put(runnableName, newLevels);
  }

  private LocalFile createLocalFile(String name, Location location, boolean archive) throws IOException {
    return new DefaultLocalFile(name, location.toURI(), location.lastModified(), location.length(), archive, null);
  }

  private void createTwillJar(final ApplicationBundler bundler,
                              Map<String, LocalFile> localFiles) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.TWILL_JAR);
    Location location = locationCache.get(Constants.Files.TWILL_JAR, new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        bundler.createBundle(targetLocation, ApplicationMasterMain.class, TwillContainerMain.class, OptionSpec.class);
      }
    });

    LOG.debug("Done {}", Constants.Files.TWILL_JAR);
    localFiles.put(Constants.Files.TWILL_JAR, createLocalFile(Constants.Files.TWILL_JAR, location, true));
  }


  private void createApplicationJar(final ApplicationBundler bundler,
                                    Map<String, LocalFile> localFiles) throws IOException {
    final Set<Class<?>> classes = Sets.newIdentityHashSet();
    classes.addAll(dependencies);

    try {
      ClassLoader classLoader = getClassLoader();
      for (RuntimeSpecification spec : twillSpec.getRunnables().values()) {
        classes.add(classLoader.loadClass(spec.getRunnableSpecification().getClassName()));
      }

      // Add the TwillRunnableEventHandler class
      if (twillSpec.getEventHandler() != null) {
        classes.add(getClassLoader().loadClass(twillSpec.getEventHandler().getClassName()));
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Cannot create application jar", e);
    }

    // The location name is computed from the MD5 of all the classes names
    // The localized name is always APPLICATION_JAR
    List<String> classList = classes.stream().map(Class::getName).sorted().collect(Collectors.toList());
    Hasher hasher = Hashing.md5().newHasher();
    for (String name : classList) {
      hasher.putString(name);
    }
    // Only depends on class list so that it can be reused across different launches
    String name = hasher.hash().toString() + "-" + Constants.Files.APPLICATION_JAR;

    LOG.debug("Create and copy {}", Constants.Files.APPLICATION_JAR);
    Location location = locationCache.get(name, new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        bundler.createBundle(targetLocation, classes);
      }
    });

    LOG.debug("Done {}", Constants.Files.APPLICATION_JAR);

    localFiles.put(Constants.Files.APPLICATION_JAR,
                   createLocalFile(Constants.Files.APPLICATION_JAR, location, true));
  }

  private void createResourcesJar(ApplicationBundler bundler, Map<String, LocalFile> localFiles,
                                  Path stagingDir) throws IOException {
    // If there is no resources, no need to create the jar file.
    if (resources.isEmpty()) {
      return;
    }

    LOG.debug("Create and copy {}", Constants.Files.RESOURCES_JAR);
    Location location = Locations.toLocation(new File(stagingDir.toFile(), Constants.Files.RESOURCES_JAR));
    bundler.createBundle(location, Collections.emptyList(), resources);
    LOG.debug("Done {}", Constants.Files.RESOURCES_JAR);
    localFiles.put(Constants.Files.RESOURCES_JAR, createLocalFile(Constants.Files.RESOURCES_JAR, location, true));
  }

  private void createRuntimeConfigJar(Path dir, Map<String, LocalFile> localFiles,
                                      Path stagingDir) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.RUNTIME_CONFIG_JAR);

    // Jar everything under the given directory, which contains different files needed by AM/runnable containers
    Location location = Locations.toLocation(new File(stagingDir.toFile(), Constants.Files.RUNTIME_CONFIG_JAR));
    try (
      JarOutputStream jarOutput = new JarOutputStream(location.getOutputStream());
      DirectoryStream<Path> stream = Files.newDirectoryStream(dir)
    ) {
      for (Path path : stream) {
        JarEntry jarEntry = new JarEntry(path.getFileName().toString());
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
        jarEntry.setSize(attrs.size());
        jarEntry.setLastAccessTime(attrs.lastAccessTime());
        jarEntry.setLastModifiedTime(attrs.lastModifiedTime());
        jarOutput.putNextEntry(jarEntry);

        Files.copy(path, jarOutput);
        jarOutput.closeEntry();
      }
    }

    LOG.debug("Done {}", Constants.Files.RUNTIME_CONFIG_JAR);
    localFiles.put(Constants.Files.RUNTIME_CONFIG_JAR,
                   createLocalFile(Constants.Files.RUNTIME_CONFIG_JAR, location, true));
  }

//  /**
//   * Based on the given {@link TwillSpecification}, copy file to local filesystem.
//   * @param spec The {@link TwillSpecification} for populating resource.
//   */
//  private Map<String, Collection<LocalFile>> populateRunnableLocalFiles(TwillSpecification spec,
//                                                                        Path stagingDir) throws Exception {
//    Map<String, Collection<LocalFile>> localFiles = new HashMap<>();
//
//    LOG.debug("Populating Runnable LocalFiles");
//    for (Map.Entry<String, RuntimeSpecification> entry : spec.getRunnables().entrySet()) {
//      String runnableName = entry.getKey();
//
//      for (LocalFile localFile : entry.getValue().getLocalFiles()) {
//        LocalFile resolvedLocalFile = resolveLocalFile(localFile, stagingDir);
//        URI remoteURI = launcher.getRemoteURI(resolvedLocalFile.getName(), resolvedLocalFile.getURI());
//        File remoteFile = new File(remoteURI.getPath());
//        DefaultLocalFile remoteLocalFile = new DefaultLocalFile(remoteFile.getName(), remoteURI,
//                                                                remoteFile.lastModified(),
//                                                                remoteFile.length(), resolvedLocalFile.isArchive(),
//                                                                resolvedLocalFile.getPattern());
//        localFiles.computeIfAbsent(runnableName, s -> new ArrayList<>()).add(remoteLocalFile);
//        LOG.info("Added file {}", remoteLocalFile.getURI());
//      }
//    }
//    LOG.debug("Done Runnable LocalFiles");
//    return localFiles;
//  }

  /**
   * Based on the given {@link TwillSpecification}, copy file to local filesystem.
   * @param spec The {@link TwillSpecification} for populating resource.
   */
  private Map<String, Collection<LocalFile>> populateRunnableLocalFiles(TwillSpecification spec,
                                                                        Path stagingDir) throws Exception {
    Map<String, Collection<LocalFile>> localFiles = new HashMap<>();

    LOG.debug("Populating Runnable LocalFiles");
    for (Map.Entry<String, RuntimeSpecification> entry : spec.getRunnables().entrySet()) {
      String runnableName = entry.getKey();

      for (LocalFile localFile : entry.getValue().getLocalFiles()) {
        LocalFile resolvedLocalFile = resolveLocalFile(localFile, stagingDir);
//        URI remoteURI = launcher.getRemoteURI(resolvedLocalFile.getName(), resolvedLocalFile.getURI());
//        File remoteFile = new File(remoteURI.getPath());
//        DefaultLocalFile remoteLocalFile = new DefaultLocalFile(remoteFile.getName(), remoteURI,
//                                                                remoteFile.lastModified(),
//                                                                remoteFile.length(), resolvedLocalFile.isArchive(),
//                                                                resolvedLocalFile.getPattern());
        localFiles.computeIfAbsent(runnableName, s -> new ArrayList<>()).add(resolvedLocalFile);
        LOG.info("Added file {}", resolvedLocalFile.getURI());
      }
    }
    LOG.debug("Done Runnable LocalFiles");
    return localFiles;
  }

  private LocalFile resolveLocalFile(LocalFile localFile, Path stagingDir) throws IOException {
    URI uri = localFile.getURI();
    String scheme = uri.getScheme();

    // If local file, resolve the last modified time and the file size
    if (scheme == null || "file".equals(scheme)) {
      File file = new File(uri.getPath());
      return new DefaultLocalFile(localFile.getName(), uri, file.lastModified(),
                                  file.length(), localFile.isArchive(), localFile.getPattern());
    }

    // If have the same scheme as the location factory, resolve time and size using Location
    if (Objects.equals(locationFactory.getHomeLocation().toURI().getScheme(), scheme)) {
      Location location = locationFactory.create(uri);
      return new DefaultLocalFile(localFile.getName(), uri, location.lastModified(),
                                  location.length(), localFile.isArchive(), localFile.getPattern());
    }

    // For other cases, attempt to save the URI content to local file, using support URLSteamHandler
    try (InputStream input = uri.toURL().openStream()) {
      Path tempFile = Files.createTempFile(stagingDir, localFile.getName(), Paths.getExtension(localFile.getName()));
      Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
      BasicFileAttributes attrs = Files.readAttributes(tempFile, BasicFileAttributes.class);
      return new DefaultLocalFile(localFile.getName(), tempFile.toUri(), attrs.lastModifiedTime().toMillis(),
                                  attrs.size(), localFile.isArchive(), localFile.getPattern());
    }
  }

  private TwillRuntimeSpecification saveSpecification(TwillSpecification spec,
                                                      Path targetFile, Path stagingDir,
                                                      Launcher launcher) throws Exception {
    final Map<String, Collection<LocalFile>> runnableLocalFiles = populateRunnableLocalFiles(spec, stagingDir);

    // Rewrite LocalFiles inside twillSpec
    Map<String, RuntimeSpecification> runtimeSpec = spec.getRunnables().entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> {
        RuntimeSpecification value = e.getValue();
        return new DefaultRuntimeSpecification(value.getName(), value.getRunnableSpecification(),
                                               value.getResourceSpecification(),
                                               runnableLocalFiles.getOrDefault(e.getKey(), Collections.emptyList()));
      }));

    // Serialize into a local temp file.
    LOG.debug("Creating {}", targetFile);
    try (Writer writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8)) {
      EventHandlerSpecification eventHandler = spec.getEventHandler();
      if (eventHandler == null) {
        eventHandler = new LogOnlyEventHandler().configure();
      }
      TwillSpecification newTwillSpec =
        new DefaultTwillSpecification(spec.getName(), runtimeSpec, spec.getOrders(),
                                      spec.getPlacementPolicies(), eventHandler);
      Map<String, String> configMap = Maps.newHashMap();
      for (Map.Entry<String, String> entry : hConf) {
        if (entry.getKey().startsWith("twill.")) {
          configMap.put(entry.getKey(), entry.getValue());
        }
      }

      TwillRuntimeSpecification twillRuntimeSpec = new TwillRuntimeSpecification(
        newTwillSpec, "", URI.create("."), "", RunIds.fromString(programRunId.getRun()), twillSpec.getName(),
        null,
        logLevels, maxRetries, configMap, runnableConfigs);
      TwillRuntimeSpecificationAdapter.create().toJson(twillRuntimeSpec, writer);
      LOG.debug("Done {}", targetFile);
      return twillRuntimeSpec;
    }
  }

  private void saveLogback(Path targetFile) throws IOException {
    URL url = getClass().getClassLoader().getResource(Constants.Files.LOGBACK_TEMPLATE);
    if (url == null) {
      return;
    }

    LOG.debug("Creating {}", targetFile);
    try (InputStream is = url.openStream()) {
      Files.copy(is, targetFile);
    }
    LOG.debug("Done {}", targetFile);
  }

  /**
   * Creates the launcher.jar for launch the main application.
   */
  private void createLauncherJar(Map<String, LocalFile> localFiles) throws IOException {

    LOG.info("Create and copy {}", Constants.Files.LAUNCHER_JAR);

    // TODO Optimize jar packaging
    Location location = locationCache.get(Constants.Files.LAUNCHER_JAR, new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        // Create a jar file with the TwillLauncher and FindFreePort and dependent classes inside.
        try (JarOutputStream jarOut = new JarOutputStream(targetLocation.getOutputStream())) {
          ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
          if (classLoader == null) {
            classLoader = getClass().getClassLoader();
          }
          Dependencies.findClassDependencies(classLoader, new ClassAcceptor() {
            @Override
            public boolean accept(String className, URL classUrl, URL classPathUrl) {
              try {
                jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
                try (InputStream is = classUrl.openStream()) {
                  ByteStreams.copy(is, jarOut);
                }
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return true;
            }
          }, WrappedLauncher.class.getName(), LauncherRunner.class.getName());
        }
      }
    });

    LOG.debug("Done {}", Constants.Files.LAUNCHER_JAR);
    localFiles.put(Constants.Files.LAUNCHER_JAR, createLocalFile(Constants.Files.LAUNCHER_JAR, location, false));
  }

  private void saveClassPaths(Path targetDir) throws IOException {
    Files.write(targetDir.resolve(Constants.Files.APPLICATION_CLASSPATH),
                Joiner.on(':').join(applicationClassPaths).getBytes(StandardCharsets.UTF_8));
    Files.write(targetDir.resolve(Constants.Files.CLASSPATH),
                Joiner.on(':').join(classPaths).getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Finds a resource from the current {@link ClassLoader} and copy the content to the given directory.
   */
  private void saveResource(Path targetDir, String resourceName) throws IOException {
    URL url = getClassLoader().getResource(resourceName);
    if (url == null) {
      // This shouldn't happen.
      throw new IOException("Failed to find script " + resourceName + " in classpath");
    }

    try (InputStream is = url.openStream()) {
      Files.copy(is, targetDir.resolve(resourceName));
    }
  }

  private JvmOptions getJvmOptions() {
    // Append runnable specific extra options.
    Map<String, String> runnableExtraOptions = this.runnableExtraOptions.entrySet()
      .stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> addClassLoaderClassName(extraOptions.isEmpty() ? e.getValue() : extraOptions + " " + e.getValue())));

    String globalOptions = addClassLoaderClassName(extraOptions);
    return new JvmOptions(globalOptions, runnableExtraOptions, debugOptions);
  }

  private void saveArguments(Arguments arguments, final Path targetPath) throws IOException {
    ArgumentsCodec.encode(arguments, () -> Files.newBufferedWriter(targetPath, StandardCharsets.UTF_8));
  }

  /**
   * Returns the context ClassLoader if there is any, otherwise, returns ClassLoader of this class.
   */
  private ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return classLoader == null ? getClass().getClassLoader() : classLoader;
  }

  private ApplicationBundler createBundler(ClassAcceptor classAcceptor, Path stagingDir) {
    return new ApplicationBundler(classAcceptor).setTempDir(stagingDir.toFile());
  }

  /**
   * Opens an {@link InputStream} that reads the content of the given {@link URI}.
   */
  private InputStream openURI(URI uri) throws IOException {
    String scheme = uri.getScheme();

    if (scheme == null || "file".equals(scheme)) {
      return new FileInputStream(uri.getPath());
    }

    // If having the same schema as the location factory, use the location factory to open the stream
    if (Objects.equals(locationFactory.getHomeLocation().toURI().getScheme(), scheme)) {
      return locationFactory.create(uri).getInputStream();
    }

    // Otherwise, fallback to using whatever supported in the JVM
    return uri.toURL().openStream();
  }

  /**
   * Returns the file name of a given {@link URI}. The file name is the last part of the path, separated by {@code /}.
   */
  private String getFileName(URI uri) {
    String path = uri.getPath();
    int idx = path.lastIndexOf('/');
    return idx >= 0 ? path.substring(idx + 1) : path;
  }
}

