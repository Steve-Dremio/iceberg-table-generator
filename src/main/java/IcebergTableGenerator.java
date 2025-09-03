/* (C)2025 */
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.parquet.Parquet;

public class IcebergTableGenerator {

    private final HadoopCatalog catalog;
    private final String warehousePath;
    private final ValueGenerator generator;
    private final TableIdentifier id;
    private Table table;
    private Transaction transaction;

    // Table-specific counters for deletion vector files per partition
    private final java.util.Map<String, java.util.concurrent.atomic.AtomicInteger>
        partitionDVCounters = new java.util.concurrent.ConcurrentHashMap<>();

    public IcebergTableGenerator(String warehousePath, Configuration conf, TableIdentifier id) {
        this.catalog = new HadoopCatalog();
        this.catalog.setConf(conf);
        this.catalog.initialize(
                "hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehousePath));
        this.warehousePath = warehousePath;
        this.generator = new ValueGenerator(42);
        this.id = id;
    }

    public IcebergTableGenerator create(Schema schema, PartitionSpec partitionSpec) {
        return create(schema, partitionSpec, ImmutableMap.of());
    }

    public IcebergTableGenerator create(
            Schema schema, PartitionSpec partitionSpec, Map<String, String> tableProperties) {
        System.out.format("Creating '%s'...\n", id.toString());
        if (catalog.tableExists(id)) {
            catalog.dropTable(id, true);
        }

        Map<String, String> updatedProperties = new HashMap<>(tableProperties);
        updatedProperties.computeIfAbsent(TableProperties.FORMAT_VERSION, k -> "2");

        table = catalog.createTable(id, schema, partitionSpec, updatedProperties);

        return this;
    }

    public void updateTablePropertiesToV3() {
        table.updateProperties().set("format-version", "3").commit();
    }

    public Table getTable() {
        return table;
    }

    public IcebergTableGenerator updateSpec(List<Term> additions, List<Term> removals) {
        UpdatePartitionSpec update = getTransaction().updateSpec();
        additions.forEach(update::addField);
        removals.forEach(update::removeField);
        update.commit();

        return this;
    }

    public <T> IcebergTableGenerator appendWithRowDelta(
            List<T> partitionValues,
            RecordGenerator<T> recordGenerator,
            int dataFilesPerPartition,
            int rowsPerDataFile,
            RowDelta rowDeltaInProgress,
            boolean commit)
            throws IOException {
        Preconditions.checkState(table != null, "create must be called first");
        URI dataDir = getDataDirectory(id);
        RowDelta rowDelta =
                rowDeltaInProgress != null ? rowDeltaInProgress : getTransaction().newRowDelta();

        for (T value : partitionValues) {
            URI partitionDir = dataDir.resolve(value.toString() + "/");
            for (int fileNum = 0; fileNum < dataFilesPerPartition; fileNum++) {
                OutputFile parquetFile =
                        getUniqueNumberedFilename(
                                partitionDir.resolve(value + "-%02d.parquet").toString());
                rowDelta.addRows(
                        writeDataFile(parquetFile, value, recordGenerator, rowsPerDataFile));
            }
        }
        if (commit) {
            rowDelta.commit();
        }
        return this;
    }

    public <T> IcebergTableGenerator append(
            List<T> partitionValues,
            RecordGenerator<T> recordGenerator,
            int dataFilesPerPartition,
            int rowsPerDataFile)
            throws IOException {
        Preconditions.checkState(table != null, "create must be called first");
        URI dataDir = getDataDirectory(id);
        AppendFiles appendFiles = getTransaction().newAppend();

        for (T value : partitionValues) {
            URI partitionDir = dataDir.resolve(value.toString() + "/");
            for (int fileNum = 0; fileNum < dataFilesPerPartition; fileNum++) {
                OutputFile parquetFile =
                        getUniqueNumberedFilename(
                                partitionDir.resolve(value + "-%02d.parquet").toString());
                appendFiles.appendFile(
                        writeDataFile(parquetFile, value, recordGenerator, rowsPerDataFile));
            }
        }

        appendFiles.commit();

        return this;
    }

    public IcebergTableGenerator append(
            RecordGenerator<Void> recordGenerator, int numDataFiles, int rowsPerDataFile)
            throws IOException {
        Preconditions.checkState(table != null, "create must be called first");
        URI dataDir = getDataDirectory(id);
        AppendFiles appendFiles = getTransaction().newAppend();

        for (int fileNum = 0; fileNum < numDataFiles; fileNum++) {
            OutputFile parquetFile =
                    getUniqueNumberedFilename(dataDir.resolve("%02d.parquet").toString());
            appendFiles.appendFile(
                    writeUnpartitionedDataFile(parquetFile, recordGenerator, rowsPerDataFile));
        }

        appendFiles.commit();

        return this;
    }

    public <T> IcebergTableGenerator appendEmptyFile(T partitionValue, Path path)
            throws IOException {
        Preconditions.checkState(table != null, "create must be called first");
        URI dataDir = getDataDirectory(id);
        AppendFiles appendFiles = getTransaction().newAppend();

        URI partitionDir = dataDir.resolve(partitionValue.toString() + "/");
        OutputFile parquetFile =
                getUniqueNumberedFilename(
                        partitionDir.resolve(partitionValue + "-%02d.parquet").toString());
        Files.copy(path, Paths.get(parquetFile.location()));

        PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
        partitionKey.set(0, partitionValue);

        DataFile dataFile =
                DataFiles.builder(table.spec())
                        .withPartition(partitionKey)
                        .withInputFile(parquetFile.toInputFile())
                        .withFormat(FileFormat.PARQUET)
                        .withRecordCount(1)
                        .build();

        appendFiles.appendFile(dataFile);
        appendFiles.commit();

        return this;
    }

    public IcebergTableGenerator positionalDelete(Predicate<Record> deletePredicate)
            throws IOException {
        return positionalDelete(null, deletePredicate, 0, 0, null, null, null);
    }

    public <T> IcebergTableGenerator deletionVectors(
            List<T> partitionValues, Predicate<Record> deletePredicate) throws IOException {
        return deletionVectors(partitionValues, deletePredicate, null);
    }

    public <T> IcebergTableGenerator deletionVectors(
            List<T> partitionValues, Predicate<Record> deletePredicate, RowDelta rowDeltaInProgress)
            throws IOException {
        URI dataDir = getDataDirectory(id);
        Expression expr =
                partitionValues != null
                        ? Expressions.in(
                                table.spec().fields().get(0).name(), partitionValues.toArray())
                        : Expressions.alwaysTrue();
        CloseableIterable<FileScanTask> scanTasks = table.newScan().filter(expr).planFiles();

        RowDelta rowDelta =
                rowDeltaInProgress != null ? rowDeltaInProgress : getTransaction().newRowDelta();

        Map<PartitionKey, List<FileScanTask>> orderedTasks =
                orderFileScanTasksByPartitionAndPath(scanTasks);
        for (PartitionKey key : orderedTasks.keySet()) {
            List<String> dataFiles =
                    orderedTasks.get(key).stream()
                            .map(t -> t.file().location())
                            .sorted(String::compareTo)
                            .collect(Collectors.toList());

            // Use a different approach: manually create deletion vector files in partition
            // directories
            // by using a custom DV writer that handles partition placement
            String partitionPath;
            if (key.size() > 0) {
                String partitionString = partitionKeyDirectoryName(key);
                URI partitionDir =
                        partitionString.length() > 0
                                ? dataDir.resolve(partitionString + "/")
                                : dataDir;
                partitionPath = partitionDir.toString();
                System.out.println(
                        "Creating deletion vector files in partition directory: " + partitionDir);
            } else {
                partitionPath = dataDir.toString();
                System.out.println("Creating deletion vector files in root directory: " + dataDir);
            }

            // Create custom DV writer that uses partition-aware file creation
            OutputFileFactory fileFactory =
                    OutputFileFactory.builderFor(table, 100, 1)
                            .format(FileFormat.PUFFIN)
                            .defaultSpec(table.spec())
                            .build();

            // Use the proper BaseDVFileWriter extension for partition-aware deletion vectors
            PartitionAwareDVFileWriter dvWriter =
                    new PartitionAwareDVFileWriter(
                            fileFactory,
                            new PreviousDeleteLoader(table, ImmutableMap.of()),
                            table.spec(),
                            key,
                            partitionKeyDirectoryName(key));

            String partitionInfo =
                    key.size() > 0 ? partitionKeyDirectoryName(key) : "unpartitioned";
            System.out.println(
                    "Creating deletion vector files directly in partition: " + partitionInfo);

            try {
                for (String path : dataFiles) {
                    try (CloseableIterable<Record> reader =
                            Parquet.read(table.io().newInputFile(path))
                                    .project(table.schema())
                                    .reuseContainers()
                                    .createReaderFunc(
                                            fileSchema ->
                                                    GenericParquetReaders.buildReader(
                                                            table.schema(), fileSchema))
                                    .build()) {

                        long pos = 0;
                        int deletedCount = 0;
                        int totalCount = 0;
                        for (Record record : reader) {
                            totalCount++;
                            if (deletePredicate.test(record)) {
                                dvWriter.delete(path, pos, table.spec(), key);
                                deletedCount++;
                                System.out.println(
                                        "Marking row " + pos + " for deletion in file: " + path);
                            }
                            pos++;
                        }
                        System.out.println(
                                "Processed "
                                        + totalCount
                                        + " records, marked "
                                        + deletedCount
                                        + " for deletion in file: "
                                        + path);
                    }
                }
                dvWriter.close();
                DeleteWriteResult res = dvWriter.result();
                List<DeleteFile> deleteFiles = res.deleteFiles();
                System.out.println("Created " + deleteFiles.size() + " deletion vector files");
                for (DeleteFile deleteFile : deleteFiles) {
                    System.out.println(
                            "Adding deletion vector file to rowDelta: "
                                    + deleteFile.path()
                                    + " (recordCount: "
                                    + deleteFile.recordCount()
                                    + ")");
                    rowDelta.addDeletes(deleteFile);
                }
            } catch (Exception e) {
                try {
                    dvWriter.close();
                } catch (Exception closeException) {
                    e.addSuppressed(closeException);
                }
                throw e;
            }
        }

        if (rowDeltaInProgress == null) {
            System.out.println("Committing rowDelta with deletion vector changes...");
            rowDelta.commit();
            System.out.println("RowDelta committed successfully!");
        } else {
            System.out.println("RowDelta not committed (will be committed later with transaction).");
        }
        return this;
    }

    public IcebergTableGenerator positionalDelete(
            Predicate<Record> deletePredicate, String specificDataFilePath) throws IOException {
        return positionalDelete(null, deletePredicate, 0, 0, null, specificDataFilePath, null);
    }

    public <T> IcebergTableGenerator positionalDelete(
            List<T> partitionValues, Predicate<Record> deletePredicate, String specificDataFilePath)
            throws IOException {
        return positionalDelete(
                partitionValues, deletePredicate, 0, 0, null, specificDataFilePath, null);
    }

    public <T> IcebergTableGenerator positionalDelete(
            List<T> partitionValues, Predicate<Record> deletePredicate) throws IOException {
        return positionalDelete(partitionValues, deletePredicate, 0, 0, null, null, null);
    }

    public <T> IcebergTableGenerator positionalDelete(
            List<T> partitionValues, Predicate<Record> deletePredicate, RowDelta rowDelta)
            throws IOException {
        return positionalDelete(partitionValues, deletePredicate, 0, 0, null, null, rowDelta);
    }

    public <T> IcebergTableGenerator positionalDelete(
            List<T> partitionValues,
            Predicate<Record> deletePredicate,
            int extraDataFileCountPerPartition,
            int extraDeleteCountPerDataFile,
            GenericRecord fakeRecord,
            String specificDataFilePath,
            RowDelta rowDeltaInProgress)
            throws IOException {
        URI dataDir = getDataDirectory(id);
        Expression expr =
                partitionValues != null
                        ? Expressions.in(
                                table.spec().fields().get(0).name(), partitionValues.toArray())
                        : Expressions.alwaysTrue();
        CloseableIterable<FileScanTask> scanTasks = table.newScan().filter(expr).planFiles();

        RowDelta rowDelta =
                rowDeltaInProgress != null ? rowDeltaInProgress : getTransaction().newRowDelta();

        Map<PartitionKey, List<FileScanTask>> orderedTasks =
                orderFileScanTasksByPartitionAndPath(scanTasks);
        for (PartitionKey key : orderedTasks.keySet()) {
            OutputFile deleteFile;
            String fakePathPrefix;
            if (key.size() > 0) {
                String partitionString = partitionKeyDirectoryName(key);
                URI partitionDir =
                        partitionString.length() > 0
                                ? dataDir.resolve(partitionString + "/")
                                : dataDir;
                deleteFile =
                        getUniqueNumberedFilename(
                                partitionDir + "/delete-" + partitionString + "-%02d.parquet");
                fakePathPrefix = partitionDir + partitionString + "-";
            } else {
                deleteFile = getUniqueNumberedFilename(dataDir + "/delete-%02d.parquet");
                fakePathPrefix = dataDir.toString();
            }

            Set<String> realDataFiles =
                    orderedTasks.get(key).stream()
                            .map(t -> t.file().location())
                            .filter(
                                    path ->
                                            specificDataFilePath == null
                                                    || path.equals(specificDataFilePath))
                            .collect(Collectors.toSet());

            // Skip if no matching files in this partition
            if (realDataFiles.isEmpty()) {
                continue;
            }

            List<String> dataFiles = new ArrayList<>(realDataFiles);
            for (int i = 0; i < extraDataFileCountPerPartition; i++) {
                String fakePath =
                        fakePathPrefix
                                + String.format("%010d-%s-fake.parquet", i, UUID.randomUUID());
                dataFiles.add(fakePath);
            }
            dataFiles.sort(String::compareTo);

            PositionDeleteWriter<Record> deleteWriter =
                    Parquet.writeDeletes(deleteFile)
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .overwrite()
                            .rowSchema(table.schema())
                            .withSpec(table.spec())
                            .withPartition(key)
                            .setAll(table.properties())
                            .buildPositionWriter();
            try (PositionDeleteWriter<Record> writer = deleteWriter) {
                for (String path : dataFiles) {
                    if (realDataFiles.contains(path)) {
                        try (CloseableIterable<Record> reader =
                                Parquet.read(table.io().newInputFile(path))
                                        .project(table.schema())
                                        .reuseContainers()
                                        .createReaderFunc(
                                                fileSchema ->
                                                        GenericParquetReaders.buildReader(
                                                                table.schema(), fileSchema))
                                        .build()) {

                            int pos = 0;
                            for (Record record : reader) {
                                if (deletePredicate.test(record)) {
                                    PositionDelete<Record> posDel = PositionDelete.create();
                                    posDel.set(path, pos, record);
                                    writer.write(posDel);
                                }
                                pos++;
                            }
                        }
                    } else {
                        for (int i = 0, pos = 0;
                                i < extraDeleteCountPerDataFile;
                                i++, pos += generator.intRange(1, 100)) {
                            PositionDelete<Record> posDel = PositionDelete.create();
                            posDel.set(path, pos);
                            writer.write(posDel);
                        }
                    }
                }
            }

            rowDelta.addDeletes(deleteWriter.toDeleteFile());
        }

        if (rowDeltaInProgress == null) {
            rowDelta.commit();
        }
        return this;
    }

    public IcebergTableGenerator equalityDelete(
            Predicate<Record> deletePredicate, List<Integer> equalityIds) throws IOException {
        return equalityDelete(null, deletePredicate, equalityIds, null);
    }

    public <T> IcebergTableGenerator equalityDelete(
            List<T> partitionValues, Predicate<Record> deletePredicate, List<Integer> equalityIds)
            throws IOException {
        return equalityDelete(partitionValues, deletePredicate, equalityIds, null);
    }

  public IcebergTableGenerator globalEqualityDelete(
      Predicate<Record> deletePredicate, List<Integer> equalityIds, Schema newSchema)
      throws IOException {
        URI dataDir = getDataDirectory(id);
        Schema schema = newSchema == null ? table.schema() : newSchema;
        Expression expr = Expressions.alwaysTrue();
        CloseableIterable<FileScanTask> scanTasks = table.newScan().filter(expr).planFiles();
    
        RowDelta rowDelta = getTransaction().newRowDelta();
        Map<PartitionKey, List<FileScanTask>> orderedTasks =
            orderFileScanTasksByPartitionAndPath(scanTasks);
        OutputFile deleteFile = getUniqueNumberedFilename(dataDir + "/eqdelete-%02d.parquet");
    
        List<String> dataFiles = new ArrayList<>();
        for (PartitionKey key : orderedTasks.keySet()) {
    
          List<String> newDataFiles =
              orderedTasks.get(key).stream()
                  .map(t -> t.file().location())
                  .sorted(String::compareTo)
                  .collect(Collectors.toList());
          dataFiles.addAll(newDataFiles);
        }
    
        EqualityDeleteWriter<Record> deleteWriter =
            Parquet.writeDeletes(deleteFile)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .rowSchema(schema)
                .withSpec(PartitionSpec.unpartitioned())
                .equalityFieldIds(equalityIds)
                .setAll(table.properties())
                .buildEqualityWriter();
        Set<Record> recordSet = new HashSet<>();
        try (EqualityDeleteWriter<Record> writer = deleteWriter) {
          for (String path : dataFiles) {
            try (CloseableIterable<Record> reader =
                Parquet.read(table.io().newInputFile(path))
                    .project(schema)
                    .reuseContainers()
                    .createReaderFunc(
                        fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                    .build()) {
              for (Record record : reader) {
                if (deletePredicate.test(record)) {
                  if (!recordSet.contains(record)) {
                      recordSet.add(record);
                      writer.write(record);
                  }
                }
              }
            }
          }
        }
    
        rowDelta.addDeletes(deleteWriter.toDeleteFile());
    
        rowDelta.commit();
        return this;
    }

    public <T> IcebergTableGenerator equalityDelete(
            List<T> partitionValues,
            Predicate<Record> deletePredicate,
            List<Integer> equalityIds,
            RowDelta rowDeltaInProgress)
            throws IOException {
        URI dataDir = getDataDirectory(id);
        Expression expr =
                partitionValues != null
                        ? Expressions.in(
                                table.spec().fields().get(0).name(), partitionValues.toArray())
                        : Expressions.alwaysTrue();
        CloseableIterable<FileScanTask> scanTasks = table.newScan().filter(expr).planFiles();

        RowDelta rowDelta =
                rowDeltaInProgress != null ? rowDeltaInProgress : getTransaction().newRowDelta();

        Map<PartitionKey, List<FileScanTask>> orderedTasks =
                orderFileScanTasksByPartitionAndPath(scanTasks);
        for (PartitionKey key : orderedTasks.keySet()) {
            OutputFile deleteFile;
            if (key.size() > 0) {
                String partitionString = partitionKeyDirectoryName(key);
                URI partitionDir =
                        partitionString.length() > 0
                                ? dataDir.resolve(partitionString + "/")
                                : dataDir;
                deleteFile =
                        getUniqueNumberedFilename(
                                partitionDir + "/eqdelete-" + partitionString + "-%02d.parquet");
            } else {
                deleteFile = getUniqueNumberedFilename(dataDir + "/eqdelete-%02d.parquet");
            }

            List<String> dataFiles =
                    orderedTasks.get(key).stream()
                            .map(t -> t.file().location())
                            .sorted(String::compareTo)
                            .collect(Collectors.toList());

            EqualityDeleteWriter<Record> deleteWriter =
                    Parquet.writeDeletes(deleteFile)
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .overwrite()
                            .rowSchema(table.schema())
                            .withSpec(table.spec())
                            .withPartition(key)
                            .equalityFieldIds(equalityIds)
                            .setAll(table.properties())
                            .buildEqualityWriter();
            try (EqualityDeleteWriter<Record> writer = deleteWriter) {
                for (String path : dataFiles) {
                    try (CloseableIterable<Record> reader =
                            Parquet.read(table.io().newInputFile(path))
                                    .project(table.schema())
                                    .reuseContainers()
                                    .createReaderFunc(
                                            fileSchema ->
                                                    GenericParquetReaders.buildReader(
                                                            table.schema(), fileSchema))
                                    .build()) {

                        for (Record record : reader) {
                            if (deletePredicate.test(record)) {
                                writer.write(record);
                            }
                        }
                    }
                }
            }

            rowDelta.addDeletes(deleteWriter.toDeleteFile());
        }

        /*if (rowDeltaInProgress == null)  {
          rowDelta.commit();
        }*/
        rowDelta.commit();
        return this;
    }

    public Transaction getTransaction() {
        if (transaction == null) {
            transaction = table.newTransaction();
        }

        return transaction;
    }

    public IcebergTableGenerator commit() {
        if (transaction != null) {
            System.out.println("Committing transaction to persist metadata changes...");
            transaction.commitTransaction();
            transaction = null;
            System.out.println("Transaction committed successfully!");
        } else {
            System.out.println("No transaction to commit.");
        }
        return this;
    }

    private URI getDataDirectory(TableIdentifier id) {
        URI uri = URI.create(warehousePath + "/");
        return uri.resolve(id.toString().replace(".", "/") + "/data/");
    }

    private OutputFile getUniqueNumberedFilename(String template) {
        int fileNum = 0;
        OutputFile name;
        do {
            name = table.io().newOutputFile(String.format(template, fileNum));
            fileNum++;
        } while (name.toInputFile().exists());

        return name;
    }

    private <T> DataFile writeDataFile(
            OutputFile parquetFile, T partitionValue, RecordGenerator<T> recordGenerator, int nrows)
            throws IOException {
        try (FileAppender<GenericRecord> appender =
                Parquet.write(parquetFile)
                        .schema(table.schema())
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .setAll(table.properties())
                        .build()) {
            Stream<GenericRecord> stream =
                    Stream.iterate(0, i -> i + 1)
                            .limit(nrows)
                            .map(i -> recordGenerator.next(generator, partitionValue));
            appender.addAll(stream.iterator());
            appender.close();

            PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
            partitionKey.set(0, partitionValue);

            return DataFiles.builder(table.spec())
                    .withPartition(partitionKey)
                    .withInputFile(parquetFile.toInputFile())
                    .withMetrics(appender.metrics())
                    .withFormat(FileFormat.PARQUET)
                    .build();
        }
    }

    private DataFile writeUnpartitionedDataFile(
            OutputFile parquetFile, RecordGenerator<Void> recordGenerator, int nrows)
            throws IOException {
        try (FileAppender<GenericRecord> appender =
                Parquet.write(parquetFile)
                        .schema(table.schema())
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .setAll(table.properties())
                        .build()) {
            Stream<GenericRecord> stream =
                    Stream.iterate(0, i -> i + 1)
                            .limit(nrows)
                            .map(i -> recordGenerator.next(generator, null));
            appender.addAll(stream.iterator());
            appender.close();

            return DataFiles.builder(table.spec())
                    .withInputFile(parquetFile.toInputFile())
                    .withMetrics(appender.metrics())
                    .withFormat(FileFormat.PARQUET)
                    .build();
        }
    }

    private Map<PartitionKey, List<FileScanTask>> orderFileScanTasksByPartitionAndPath(
            CloseableIterable<FileScanTask> scanTasks) {
        Map<PartitionKey, List<FileScanTask>> map = new HashMap<>();
        for (FileScanTask task : scanTasks) {
            PartitionKey key = getPartitionKey(task);
            map.computeIfAbsent(key, k -> new ArrayList<>()).add(task);
        }

        for (PartitionKey key : map.keySet()) {
            map.get(key).sort(Comparator.comparing(t -> t.file().location()));
        }

        return map;
    }

    private PartitionKey getPartitionKey(FileScanTask scanTask) {
        PartitionKey partitionKey = new PartitionKey(scanTask.spec(), table.schema());
        for (int i = 0; i < scanTask.file().partition().size(); i++) {
            partitionKey.set(i, scanTask.file().partition().get(i, Object.class));
        }

        return partitionKey;
    }

    private String partitionKeyDirectoryName(PartitionKey partitionKey) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < partitionKey.size(); i++) {
            if (i > 0) {
                builder.append("-");
            }
            builder.append(partitionKey.get(i, Object.class));
        }

        return builder.toString();
    }

    // Custom DV writer that extends BaseDVFileWriter and overrides close() for partition-aware file
    // creation
    private class PartitionAwareDVFileWriter extends BaseDVFileWriter {
        private final OutputFileFactory fileFactory;
        private final PartitionSpec spec;
        private final PartitionKey partitionKey;
        private final String partitionString;

        PartitionAwareDVFileWriter(
                OutputFileFactory fileFactory,
                Function<String, PositionDeleteIndex> previousDeleteLoader,
                PartitionSpec spec,
                PartitionKey partitionKey,
                String partitionString) {
            super(fileFactory, previousDeleteLoader);
            this.fileFactory = fileFactory;
            this.spec = spec;
            this.partitionKey = partitionKey;
            this.partitionString = partitionString;
            System.out.println(
                    "Created PartitionAwareDVFileWriter for partition: " + partitionString);
        }

        @Override
        public void close() throws IOException {
            System.out.println(
                    "PartitionAwareDVFileWriter.close() called for partition: " + partitionString);

            if (partitionKey.size() > 0) {
                // Get or create counter for this partition (starts at 0) from table-level counters
                java.util.concurrent.atomic.AtomicInteger counter =
                        partitionDVCounters.computeIfAbsent(
                                partitionString,
                                k -> new java.util.concurrent.atomic.AtomicInteger(0));

                // Get current counter value and then increment for next time
                int fileNumber = counter.getAndIncrement();

                // Create a partition-aware path using the table's IO directly
                String partitionPath = createPartitionAwarePath(fileNumber);

                System.out.println(
                        "Creating deletion vector file #"
                                + fileNumber
                                + " in partition directory: "
                                + partitionPath);

                // Call the private close method with the partition-aware path
                close(partitionPath);
            } else {
                // Use default file creation for unpartitioned tables
                System.out.println(
                        "Creating deletion vector file in table root for unpartitioned table");
                super.close();
            }
        }

        /**
         * Creates a partition-aware path using the table's data directory structure.
         */
        private String createPartitionAwarePath(int fileNumber) {
            // Get the table's data directory
            URI dataDir = getDataDirectory(id);

            // Create partition directory path (just the partition value, not category=value)
            URI partitionDir = dataDir.resolve(partitionString + "/");

            // Generate unique filename with counter
            String filename = String.format("dv-%s-%03d.puffin", partitionString, fileNumber);

            return partitionDir.resolve(filename).toString();
        }

        // Private method that uses the new close(EncryptedOutputFile) method from BaseDVFileWriter
        private void close(String customPath) throws IOException {
            // Create an OutputFile for the custom path
            OutputFile outputFile = table.io().newOutputFile(customPath);

            // Use EncryptedFiles to create a proper EncryptedOutputFile
            EncryptedOutputFile encryptedOutputFile = EncryptedFiles.encryptedOutput(outputFile, (byte[]) null);

            System.out.println(
                    "Calling BaseDVFileWriter.close(EncryptedOutputFile) with custom path: "
                            + customPath);

            // Use the new close(EncryptedOutputFile) method from your forked dependency
            close(encryptedOutputFile);

            System.out.println("Successfully used close(EncryptedOutputFile) method");
        }
    }

    private static class PreviousDeleteLoader implements Function<String, PositionDeleteIndex> {
        private final Map<String, DeleteFile> deleteFiles;
        private final DeleteLoader deleteLoader;

        PreviousDeleteLoader(Table table, Map<String, DeleteFile> deleteFiles) {
            this.deleteFiles = deleteFiles;
            this.deleteLoader =
                    new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile));
        }

        @Override
        public PositionDeleteIndex apply(String path) {
            DeleteFile deleteFile = deleteFiles.get(path);
            if (deleteFile == null) {
                return null;
            }
            return deleteLoader.loadPositionDeletes(ImmutableList.of(deleteFile), path);
        }
    }
}
