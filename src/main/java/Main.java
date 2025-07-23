/* (C)2025 */
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

public class Main {

    @Parameter(
            names = {"--warehouse"},
            description = "Warehouse path")
    private String warehousePath = Paths.get(System.getenv("HOME"), "warehouse").toString();

    @Parameter(
            names = {"--conf"},
            description = "Additional hadoop configuration key/value pairs")
    private List<String> hadoopConf = ImmutableList.of();

    private Configuration conf;

    private static final Schema IDS_SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    private static final Schema ORDERS_SCHEMA =
            new Schema(
                    Types.NestedField.required(1, "order_id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "order_year", Types.IntegerType.get()),
                    Types.NestedField.required(3, "order_date", Types.TimestampType.withoutZone()),
                    Types.NestedField.required(4, "source_id", Types.IntegerType.get()),
                    Types.NestedField.required(5, "product_name", Types.StringType.get()),
                    Types.NestedField.required(6, "amount", Types.DoubleType.get()));

    private static final List<String> PRODUCT_NAMES = ImmutableList.of("Widget", "Gizmo", "Gadget");

    private static final Schema PRODUCTS_SCHEMA =
            new Schema(
                    Types.NestedField.required(1, "product_id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get()),
                    Types.NestedField.required(3, "category", Types.StringType.get()),
                    Types.NestedField.required(4, "color", Types.StringType.get()),
                    Types.NestedField.required(5, "created_date", Types.DateType.get()),
                    Types.NestedField.required(6, "weight", Types.DoubleType.get()),
                    Types.NestedField.required(7, "quantity", Types.IntegerType.get()));

    private static final List<String> PRODUCT_NAME_TEMPLATES =
            ImmutableList.of(
                    "Core%s",
                    "%sPress", "%sLab", "Ever%s", "%sScope", "%sKit", "%sTron", "%sView", "%sBuddy",
                    "Home%s");

    private static final List<String> PRODUCT_SUFFIXES =
            ImmutableList.of("", "", "Advanced", "1000", "2000", "Deluxe", "Express", "Ultimate");

    private static final List<String> COLORS =
            ImmutableList.of(
                    "black", "white", "red", "orange", "yellow", "green", "blue", "purple", "brown",
                    "gray");

    private static final int WIDE_METRICS_N_COLS = 1000;
    private static final Schema WIDE_METRICS_SCHEMA =
            new Schema(
                    Stream.iterate(0, i -> i + 1)
                            .limit(WIDE_METRICS_N_COLS)
                            .map(
                                    i ->
                                            i == 0
                                                    ? Types.NestedField.required(
                                                            i + 1, "id", Types.IntegerType.get())
                                                    : Types.NestedField.required(
                                                            i + 1,
                                                            "metric_" + (i - 1),
                                                            Types.DoubleType.get()))
                            .collect(Collectors.toList()));

    private static final Map<String, String> MERGE_ON_READ_MAP;

    static {
        MERGE_ON_READ_MAP = new HashMap<>();
        MERGE_ON_READ_MAP.put("write.delete.mode", "merge-on-read");
        MERGE_ON_READ_MAP.put("write.update.mode", "merge-on-read");
        MERGE_ON_READ_MAP.put("write.merge.mode", "merge-on-read");
    }

    public static void main(String[] args) {
        try {
            Main main = new Main();
            JCommander.newBuilder().addObject(main).build().parse(args);

            main.initHadoopConfFromArgs();
            main.run();
        } catch (Exception ex) {
            ExceptionUtils.printRootCauseStackTrace(ex, System.err);
        }
    }

    private void initHadoopConfFromArgs() {
        conf = new Configuration();
        conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        conf.set("google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");

        for (String arg : hadoopConf) {
            int idx = arg.indexOf('=');
            if (idx == -1) {
                System.err.format("Invalid --conf option: %s", arg);
                System.exit(1);
            }

            String key = arg.substring(0, idx);
            String value = arg.substring(idx + 1);

            conf.set(key, value);
        }
    }

    private void run() throws IOException {
        /*createSmallOrders();
        createSmallOrdersWithDeletes();
        //createMultiRowGroupOrdersWithDeletes();
        //createMultiRowGroupOrdersWithDeletesCopyA();
        createMultiRowGroupOrdersWithDeletesCopyB();
        createUnpartitionedOrdersWithDeletes();
        createProductsWithEqDeletes();
        createMergeOnReadTargetPartitioned();
        createMergeOnReadTargetUnpartitioned();
        createMergeOnReadSource();*/
        // createDvs();
        // createProductsWithEqDeletesAndPosDeletesSameSequenceNumber();
        // createProductsWithEqDeletesAndOverlappingPosDeletes();
        createProductsWithDeletionVectorsOnly();
        createProductsWithDeletionVectorsAndEqualityDeletes();
        createProductsWithDeletionVectorsAndV2PositionDeletes();
        createProductsWithDeletionVectorsEqualityDeletesAndV2PositionDeletes();

        // Test the new partition-aware deletion vectors method
        // testPartitionAwareDeletionVectors();

        // createProductsWithEqDeletes();
        // createWideMetrics();

        //    createProductsWithEqDeletesSchemaChange();
        //    createSmallOrdersWithLargeDeleteFile();
        //    createSmallOrdersWithPartitionEvolution();

        // createMultiRowGroupOrdersWithDeletes();
        // createMultiRowGroupOrdersWithDeletesCopyA();
    }

    private void createMergeOnReadTargetPartitioned() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of("Merge_On_Read_Target_Partitioned"));
        tableGenerator
                .create(
                        ORDERS_SCHEMA,
                        PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build(),
                        MERGE_ON_READ_MAP)
                .append(ImmutableList.of(2024), this::generateOrdersRecord, 1, 100000)
                .commit();
    }

    private void createMergeOnReadTargetUnpartitioned() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of("Merge_On_Read_Target_Unpartitioned"));
        tableGenerator
                .create(ORDERS_SCHEMA, PartitionSpec.unpartitioned(), MERGE_ON_READ_MAP)
                .append(this::generateUnpartitionedOrdersRecord, 1, 100000)
                .commit();
    }

    private void createMergeOnReadSource() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of("Merge_On_Read_Source_Partitioned"));
        tableGenerator
                .create(
                        ORDERS_SCHEMA,
                        PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build(),
                        MERGE_ON_READ_MAP)
                .append(ImmutableList.of(2020), this::generateOrdersRecord, 1, 100000)
                .commit();
    }

    private void createSmallOrders() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(warehousePath, conf, TableIdentifier.of("orders"));
        tableGenerator
                .create(
                        ORDERS_SCHEMA,
                        PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build())
                .append(ImmutableList.of(2019, 2020), this::generateOrdersRecord, 2, 100)
                .commit()
                .append(ImmutableList.of(2021), this::generateOrdersRecord, 2, 100)
                .commit();
    }

    private void createSmallOrdersWithDeletes() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath, conf, TableIdentifier.of("orders_with_deletes"));
        tableGenerator
                .create(
                        ORDERS_SCHEMA,
                        PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build())
                .append(ImmutableList.of(2019, 2020), this::generateOrdersRecord, 2, 100)
                .commit()
                .positionalDelete(
                        ImmutableList.of(2019, 2020), r -> r.get(0, Integer.class) % 10 == 0)
                .commit()
                .append(ImmutableList.of(2020, 2021), this::generateOrdersRecord, 2, 100)
                .commit()
                .positionalDelete(
                        ImmutableList.of(2019, 2020), r -> r.get(0, Integer.class) % 10 == 3)
                .commit()
                .appendEmptyFile(
                        2021,
                        Paths.get(
                                "/Users/scott.cowell/scripts/datagen/parquet/empty-rowgroup.parquet"))
                .commit()
                .positionalDelete(ImmutableList.of(2021), r -> r.get(0, Integer.class) % 10 == 6)
                .commit();
    }

    private void createMultiRowGroupOrdersWithDeletes() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of("multi_rowgroup_orders_with_deletes"));
        tableGenerator
                .create(
                        ORDERS_SCHEMA,
                        PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build(),
                        ImmutableMap.of(
                                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
                                        Integer.toString(16 * 1024),
                                TableProperties.PARQUET_PAGE_SIZE_BYTES, Integer.toString(4 * 1024),
                                TableProperties.PARQUET_DICT_SIZE_BYTES,
                                        Integer.toString(4 * 1024)))
                .append(ImmutableList.of(2019, 2020, 2021), this::generateOrdersRecord, 3, 1000)
                .commit()
                .positionalDelete(ImmutableList.of(2021), r -> r.get(0, Integer.class) % 10 < 3)
                .commit()
                .positionalDelete(
                        ImmutableList.of(2021),
                        r -> r.get(0, Integer.class) % 10 > 0 && r.get(0, Integer.class) % 100 == 5)
                .commit()
                .positionalDelete(
                        ImmutableList.of(2020, 2021),
                        r ->
                                r.get(0, Integer.class) % 3000 >= 700
                                        && r.get(0, Integer.class) % 3000 < 1200)
                .commit();
    }

    private void createMultiRowGroupOrdersWithDeletesCopyA() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of("multi_rowgroup_orders_with_deletes_copy_A"));
        tableGenerator
                .create(
                        ORDERS_SCHEMA,
                        PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build(),
                        ImmutableMap.of(
                                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
                                        Integer.toString(16 * 1024),
                                TableProperties.PARQUET_PAGE_SIZE_BYTES, Integer.toString(4 * 1024),
                                TableProperties.PARQUET_DICT_SIZE_BYTES,
                                        Integer.toString(4 * 1024)))
                .append(ImmutableList.of(2019, 2020, 2021), this::generateOrdersRecord, 3, 1000)
                .commit()
                .positionalDelete(ImmutableList.of(2021), r -> r.get(0, Integer.class) % 10 < 3)
                .commit()
                .positionalDelete(
                        ImmutableList.of(2021),
                        r -> r.get(0, Integer.class) % 10 > 0 && r.get(0, Integer.class) % 100 == 5)
                .commit()
                .positionalDelete(
                        ImmutableList.of(2020, 2021),
                        r ->
                                r.get(0, Integer.class) % 3000 >= 700
                                        && r.get(0, Integer.class) % 3000 < 1200)
                .commit();
    }

    private void createMultiRowGroupOrdersWithDeletesCopyB() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of("multi_rowgroup_orders_with_deletes_copy_B"));
        tableGenerator
                .create(
                        ORDERS_SCHEMA,
                        PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build(),
                        ImmutableMap.of(
                                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
                                        Integer.toString(16 * 1024),
                                TableProperties.PARQUET_PAGE_SIZE_BYTES, Integer.toString(4 * 1024),
                                TableProperties.PARQUET_DICT_SIZE_BYTES,
                                        Integer.toString(4 * 1024)))
                .append(ImmutableList.of(2019, 2020, 2021), this::generateOrdersRecord, 3, 1000)
                .commit()
                .positionalDelete(ImmutableList.of(2021), r -> r.get(0, Integer.class) % 10 < 3)
                .commit()
                .positionalDelete(
                        ImmutableList.of(2021),
                        r -> r.get(0, Integer.class) % 10 > 0 && r.get(0, Integer.class) % 100 == 5)
                .commit()
                .positionalDelete(
                        ImmutableList.of(2020, 2021),
                        r ->
                                r.get(0, Integer.class) % 3000 >= 700
                                        && r.get(0, Integer.class) % 3000 < 1200)
                .commit();
    }

    private void createOrdersFullRowgroupDelete() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath, conf, TableIdentifier.of("orders_full_rowgroup_delete"));
        tableGenerator
                .create(
                        ORDERS_SCHEMA,
                        PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build(),
                        ImmutableMap.of(
                                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
                                        Integer.toString(16 * 1024),
                                TableProperties.PARQUET_PAGE_SIZE_BYTES, Integer.toString(4 * 1024),
                                TableProperties.PARQUET_DICT_SIZE_BYTES,
                                        Integer.toString(4 * 1024)))
                .append(ImmutableList.of(2019, 2020, 2021), this::generateOrdersRecord, 3, 1000)
                .commit()
                .positionalDelete(ImmutableList.of(2019), r -> r.get(0, Integer.class) < 900)
                .commit();
    }

    private void createOrdersWithLongPaths() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of(
                                "orders_with_a_table_name_that_is_longer_than_two_hundred_and_fifty_six_characters_so_that_i_can_reproduce_a_bug_due_to_very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_long_paths"));
        tableGenerator
                .create(
                        ORDERS_SCHEMA,
                        PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build(),
                        ImmutableMap.of(
                                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
                                        Integer.toString(16 * 1024),
                                TableProperties.PARQUET_PAGE_SIZE_BYTES, Integer.toString(4 * 1024),
                                TableProperties.PARQUET_DICT_SIZE_BYTES,
                                        Integer.toString(4 * 1024)))
                .append(ImmutableList.of(2019, 2020, 2021), this::generateOrdersRecord, 3, 1000)
                .commit()
                .positionalDelete(ImmutableList.of(2019), r -> r.get(0, Integer.class) < 900)
                .commit();
    }

    private void createSmallOrdersWithLargeDeleteFile() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath, conf, TableIdentifier.of("orders_with_large_delete_file"));
        tableGenerator
                .create(
                        ORDERS_SCHEMA,
                        PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build())
                .append(ImmutableList.of(2021), this::generateOrdersRecord, 2, 100)
                .commit()
                .positionalDelete(
                        ImmutableList.of(2021),
                        r -> r.get(0, Integer.class) % 10 < 3,
                        10000,
                        10000,
                        getFakeOrdersRecordForExtraDeletes(),
                        null,
                        null);
    }

    private void createSmallOrdersWithPartitionEvolution() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath, conf, TableIdentifier.of("orders_part_evol"));
        tableGenerator
                .create(
                        ORDERS_SCHEMA,
                        PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build())
                .append(ImmutableList.of(2019, 2020), this::generateOrdersRecord, 2, 100)
                .commit()
                .append(ImmutableList.of(2021), this::generateOrdersRecord, 2, 100)
                .commit()
                .updateSpec(
                        ImmutableList.of(Expressions.ref("source_id")),
                        ImmutableList.of(Expressions.ref("order_year")))
                .commit()
                .append(
                        ImmutableList.of(0, 1, 2, 3, 4),
                        this::generateOrdersRecordWithSourceIdPartition,
                        1,
                        40)
                .commit();
    }

    private void createUnpartitionedOrdersWithDeletes() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of("unpartitioned_orders_with_deletes"));
        tableGenerator
                .create(ORDERS_SCHEMA, PartitionSpec.unpartitioned())
                .append(this::generateUnpartitionedOrdersRecord, 2, 100)
                .commit()
                .positionalDelete(r -> r.get(0, Integer.class) % 10 == 0)
                .commit()
                .append(this::generateUnpartitionedOrdersRecord, 2, 100)
                .commit()
                .positionalDelete(r -> r.get(0, Integer.class) % 10 == 3)
                .commit();
    }

    private void createLargeUnpartitionedOrdersWithDeletes() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of("large_unpartitioned_orders_with_deletes"));
        tableGenerator
                .create(ORDERS_SCHEMA, PartitionSpec.unpartitioned())
                .append(this::generateUnpartitionedOrdersRecord, 100, 100)
                .commit();

        for (int i = 0; i < 100; i++) {
            final int x = i;
            tableGenerator.positionalDelete(r -> r.get(0, Integer.class) % 1000 == x).commit();
        }
    }

    /**
     * Creates a 'products_with_eq_deletes' table with 3 partitions on the category column: widget,
     * gadget, gizmo. 5 data files with 200 rows each are created - 2 in widget, 2 in gizmo, 1 in
     * gadget. Each data file has two row groups, each with 100 rows.
     *
     * <p>
     *
     * <p>Creation steps:
     *
     * <p>
     *
     * <pre>
     * 1. Insert 200 rows with category 'widget', product_ids from [ 0 .. 199 ].            Total rows: 200
     * 2. Delete product_ids [ 0 .. 29 ] via equality delete on product_id.                 Total rows: 170
     * 3. Insert 200 rows with category 'gizmo', product_ids from [ 200 .. 399 ].           Total rows: 370
     * 4. Delete all products with color 'green' via equality delete on color.              Total rows: 333
     * 5. Insert 600 rows, 200 each in categories 'widget', 'gizmo', 'gadget' with          Total rows: 933
     *    product_ids [ 400 .. 999]
     * 6. Delete product_ids [ 100 .. 199 ], [ 300 .. 399 ], [ 500 .. 599 ],                Total rows: 453
     *    [ 700 .. 799 ], [ 900 .. 999 ] via equality delete on product_id.
     * 7. Delete product_ids [ 50 .. 52 ] via positional delete.                            Total rows: 450
     *
     * Total rows added   : 1000
     * Total rows deleted : 550 (547 via equality delete, 3 via position delete)
     * Final row count    : 450
     * </pre>
     */
    private void createProductsWithEqDeletes() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath, conf, TableIdentifier.of("products_with_equality_deletes"));
        tableGenerator
                .create(
                        PRODUCTS_SCHEMA,
                        PartitionSpec.builderFor(PRODUCTS_SCHEMA).identity("category").build(),
                        ImmutableMap.of(
                                // Iceberg will write at minimum 100 rows per rowgroup, so set row
                                // group size small
                                // enough to
                                // guarantee that happens
                                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1)))
                // add 200 rows to widget partition
                .append(ImmutableList.of("widget"), this::generateProductsRecord, 1, 200)
                .commit()
                // delete product_ids [ 0 .. 29 ] via equality delete - 30 rows removed
                .equalityDelete(
                        ImmutableList.of("widget"),
                        r -> r.get(0, Integer.class) < 30,
                        equalityIds(PRODUCTS_SCHEMA, "product_id"))
                .commit()
                // add 200 rows to gizmo partition
                .append(ImmutableList.of("gizmo"), this::generateProductsRecord, 1, 200)
                .commit()
                // delete all products with color 'green' via equality delete - 37 rows removed
                .equalityDelete(
                        ImmutableList.of("widget", "gizmo"),
                        r -> r.get(3, String.class).equals("green"),
                        equalityIds(PRODUCTS_SCHEMA, "color"))
                .commit()
                // add 200 rows each to widget, gadget, and gizmo partitions
                .append(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        this::generateProductsRecord,
                        1,
                        200)
                .commit()
                // delete product ids [ 100 .. 199 ], [ 300 .. 399 ], [ 500 .. 599 ], [ 700 .. 799
                // ], [ 900
                // .. 999 ]
                // taking into account previous deletions this deletes 480 rows
                .equalityDelete(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        r -> r.get(0, Integer.class) % 200 >= 100,
                        equalityIds(PRODUCTS_SCHEMA, "product_id"))
                .commit();
    }

    /**
     * Creates a 'products_with_eq_deletes' table with 3 partitions on the category column: widget,
     * gadget, gizmo. 5 data files with 200 rows each are created - 2 in widget, 2 in gizmo, 1 in
     * gadget. Each data file has two row groups, each with 100 rows.
     *
     * <p>
     *
     * <p>Creation steps:
     *
     * <p>
     *
     * <pre>
     * 1. Insert 200 rows with category 'widget', product_ids from [ 0 .. 199 ].            Total rows: 200
     * 2. Delete product_ids [ 25 .. 39 via positional delete                               Total rows: 185
     * 3. Delete product_ids [ 0 .. 29 ] via equality delete on product_id. (5 Overlapping) Total rows: 160
     * 3. Insert 200 rows with category 'gizmo', product_ids from [ 200 .. 399 ].           Total rows: 360
     * 4. Delete all products with color 'green' via equality delete on color.              Total rows: 323
     * 5. Insert 600 rows, 200 each in categories 'widget', 'gizmo', 'gadget' with          Total rows: 923
     *    product_ids [ 400 .. 999]
     * 7. Delete product_ids [ 75 .. 124 ] via positional delete.                           Total rows: 879
     *....(2 overlapping from 100-125 from color deletion from earlier)
     *    (3 overlapping from 75-99 from color deletion from earlier)
     * 6. Delete product_ids [ 100 .. 199 ], [ 300 .. 399 ], [ 500 .. 599 ],                Total rows: 398
     *    [ 700 .. 799 ], [ 900 .. 999 ] via equality delete on product_id.
     *    (25 overlapping from 100-125)
     *
     * Total rows added   : 1000
     * Total rows deleted : 585 (547 via equality delete, 65 via position delete, 30 Overlapping)
     * Final row count    : 398
     * </pre>
     */
    private void createProductsWithEqDeletesAndOverlappingPosDeletes() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of("products_with_eq_deletes_and_pos_deletes_upsert"));
        tableGenerator
                .create(
                        PRODUCTS_SCHEMA,
                        PartitionSpec.builderFor(PRODUCTS_SCHEMA).identity("category").build(),
                        ImmutableMap.of(
                                // Iceberg will write at minimum 100 rows per rowgroup, so set row
                                // group size small
                                // enough to
                                // guarantee that happens
                                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1)))
                // add 200 rows to widget partition
                .append(ImmutableList.of("widget"), this::generateProductsRecord, 1, 200)
                .commit()
                // delete product_ids [ 25 .. 39 via positional delete -  15 rows removed
                .positionalDelete(
                        ImmutableList.of("widget"),
                        r -> r.get(0, Integer.class) >= 25 && r.get(0, Integer.class) < 40)
                .commit()
                // delete product_ids [ 0 .. 29 ] via equality delete - 25 rows removed with 5
                // overlapped from pos del commit
                .equalityDelete(
                        ImmutableList.of("widget"),
                        r -> r.get(0, Integer.class) < 30,
                        equalityIds(PRODUCTS_SCHEMA, "product_id"))
                .commit()
                // add 200 rows to gizmo partition
                .append(ImmutableList.of("gizmo"), this::generateProductsRecord, 1, 200)
                .commit()
                // delete all products with color 'green' via equality delete - 40 rows removed
                .equalityDelete(
                        ImmutableList.of("widget", "gizmo"),
                        r -> r.get(3, String.class).equals("green"),
                        equalityIds(PRODUCTS_SCHEMA, "color"))
                .commit()
                // add 200 rows each to widget, gadget, and gizmo partitions
                .append(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        this::generateProductsRecord,
                        1,
                        200)
                .commit()
                // delete product_ids [ 75 ... 125 ] via positional delete - 50 rows removed
                .positionalDelete(
                        ImmutableList.of("widget"),
                        r -> r.get(0, Integer.class) >= 75 && r.get(0, Integer.class) < 125)
                .commit();

        RowDelta rowDelta = tableGenerator.getTransaction().newRowDelta();
        tableGenerator
                // delete product ids [ 100 .. 199 ], [ 300 .. 399 ], [ 500 .. 599 ], [ 700 .. 799
                // ], [ 900
                // .. 999 ]
                // taking into account previous deletions this deletes 455 rows with 25 overlapped
                // from pos del commit
                .positionalDelete(
                        ImmutableList.of("widget"),
                        r -> r.get(0, Integer.class) >= 450 && r.get(0, Integer.class) < 475,
                        rowDelta)
                .equalityDelete(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        r -> r.get(0, Integer.class) % 200 >= 100,
                        equalityIds(PRODUCTS_SCHEMA, "product_id"),
                        rowDelta)
                .commit();
    }

    private void createProductsWithEqDeletesSchemaChange() throws IOException {
        Schema initialSchema = PRODUCTS_SCHEMA.select("product_id", "name", "category");
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath, conf, TableIdentifier.of("products_with_schema_change"));
        tableGenerator
                .create(
                        initialSchema,
                        PartitionSpec.builderFor(initialSchema).identity("category").build(),
                        ImmutableMap.of(
                                // Iceberg will write at minimum 100 rows per rowgroup, so set row
                                // group size small
                                // enough to
                                // guarantee that happens
                                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1)))
                // add 200 rows to widget partition
                .append(
                        ImmutableList.of("widget"),
                        createProductsRecordGenerator(tableGenerator),
                        1,
                        200)
                .commit()
                // delete product_ids [ 0 .. 29 ] via equality delete - 30 rows removed
                .equalityDelete(
                        ImmutableList.of("widget"),
                        r -> r.get(0, Integer.class) < 30,
                        equalityIds(PRODUCTS_SCHEMA, "product_id"))
                .commit();

        // add color column, remove product_id column
        UpdateSchema updateSchema = tableGenerator.getTransaction().updateSchema();
        updateSchema.addColumn("color", Types.StringType.get());
        updateSchema.deleteColumn("product_id");
        updateSchema.commit();

        tableGenerator
                // add 200 rows to gizmo partition
                .append(
                        ImmutableList.of("gizmo"),
                        createProductsRecordGenerator(tableGenerator),
                        1,
                        200)
                .commit();

        /*
           // delete all products with color 'green' via equality delete
           .equalityDelete(ImmutableList.of("widget", "gizmo"), r -> r.get(3, String.class).equals("green"),
               equalityIds(PRODUCTS_SCHEMA, "color"))
           .commit();

        */
    }

    private void createWideMetrics() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(warehousePath, conf, TableIdentifier.of("wide_metrics"));
        tableGenerator
                .create(WIDE_METRICS_SCHEMA, PartitionSpec.unpartitioned())
                .append(this::generateWideMetricsRecord, 30, 1)
                .commit();
    }

    private GenericRecord generateIdsRecord(ValueGenerator generator) {
        GenericRecord record = GenericRecord.create(IDS_SCHEMA);
        record.set(0, generator.id() * 100);
        return record;
    }

    private GenericRecord generateOrdersRecord(ValueGenerator generator, Integer partitionValue) {
        GenericRecord record = GenericRecord.create(ORDERS_SCHEMA);
        record.set(0, generator.id());
        record.set(1, partitionValue);
        record.set(2, generator.timestamp(partitionValue));
        record.set(3, generator.intRange(0, 5));
        record.set(4, generator.select(PRODUCT_NAMES) + " " + generator.intRange(0, 100));
        record.set(5, generator.doubleRange(0, 100));
        return record;
    }

    private GenericRecord generateOrdersRecordWithNegativeId(
            ValueGenerator generator, Integer partitionValue) {
        GenericRecord record = GenericRecord.create(ORDERS_SCHEMA);
        record.set(0, -1 * generator.id());
        record.set(1, partitionValue);
        record.set(2, generator.timestamp(partitionValue));
        record.set(3, generator.intRange(0, 5));
        record.set(4, generator.select(PRODUCT_NAMES) + " " + generator.intRange(0, 100));
        record.set(5, generator.doubleRange(0, 100));
        return record;
    }

    private GenericRecord generateOrdersRecordWithSourceIdPartition(
            ValueGenerator generator, Integer partitionValue) {
        int orderYear = generator.intRange(2019, 2022);
        GenericRecord record = GenericRecord.create(ORDERS_SCHEMA);
        record.set(0, generator.id());
        record.set(1, orderYear);
        record.set(2, generator.timestamp(orderYear));
        record.set(3, partitionValue);
        record.set(4, generator.select(PRODUCT_NAMES) + " " + generator.intRange(0, 100));
        record.set(5, generator.doubleRange(0, 100));
        return record;
    }

    private GenericRecord generateUnpartitionedOrdersRecord(ValueGenerator generator, Void unused) {
        GenericRecord record = GenericRecord.create(ORDERS_SCHEMA);
        int orderYear = generator.intRange(2019, 2022);
        record.set(0, generator.id());
        record.set(1, orderYear);
        record.set(2, generator.timestamp(orderYear));
        record.set(3, generator.intRange(0, 5));
        record.set(4, generator.select(PRODUCT_NAMES) + " " + generator.intRange(0, 100));
        record.set(5, generator.doubleRange(0, 100));
        return record;
    }

    /**
     * Creates a 'products_with_deletion_vectors' table with 3 partitions on the category column: widget,
     * gadget, gizmo. 5 data files with 200 rows each are created - 2 in widget, 2 in gizmo, 1 in
     * gadget. Each data file has two row groups, each with 100 rows.
     *
     * <p>
     *
     * <p>Creation steps:
     *
     * <p>
     *
     * <pre>
     * 1. Insert 200 rows with category 'widget', product_ids from [ 0 .. 199 ].            Total rows: 200
     * 2. Delete product_ids [ 25 .. 39 ] via deletion vector                               Total rows: 185 (15 rows removed by DV)
     * 3. Delete product_ids [ 0 .. 29 ] via equality delete on product_id. (5 Overlapping) Total rows: 160 (25 rows removed by EQ Delete) (5 additional overlapped)
     * 4. Insert 200 rows with category 'gizmo', product_ids from [ 200 .. 399 ].           Total rows: 360 (200 rows added)
     * 5. Delete all products with color 'green' via equality delete on color.              Total rows: 324 (36 rows removed by EQ Delete) (4 additional overlapped from 5...15...25..35)
     * 6. Insert 200 rows each to widget, gadget, and gizmo partitions,
     *    product_ids from [ 400 .. 599 ], [ 600 .. 799 ], [ 800 .. 999 ].                  Total rows: 924 (600 rows added)
     * 7. Delete product ids [ 500 .. 599 ], [ 700 .. 799 ], [ 900 .. 999 ]
     *    via DVs 300 rows removed                                                          Total rows: 624 (300 rows removed by DVs)
     *
     * Total rows inserted: 1000
     * Total rows deleted : 376 (315 DVs, 70 Eq Deletes, 11 overlapping)
     * Final row count    : 624
     * </pre>
     */
    private void createProductsWithDeletionVectorsAndEqualityDeletes() throws IOException {
        // Create a properties map with format version 3 for deletion vectors
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(TableProperties.FORMAT_VERSION, "2");
        tableProperties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1));

        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of("products_with_deletion_vectors_and_equality_deletes"));
        tableGenerator
                .create(
                        PRODUCTS_SCHEMA,
                        PartitionSpec.builderFor(PRODUCTS_SCHEMA).identity("category").build(),
                        tableProperties)
                // add 200 rows to widget partition
                .append(ImmutableList.of("widget"), this::generateProductsRecord, 1, 200)
                .commit();

        // Update to format version 3 to enable deletion vectors
        tableGenerator.updateTablePropertiesToV3();

        tableGenerator
                // delete product_ids [ 25 .. 39 via deletion vector -  15 rows removed
                .deletionVectors(
                        ImmutableList.of("widget"),
                        r -> r.get(0, Integer.class) >= 25 && r.get(0, Integer.class) < 40)
                .commit()
                // delete product_ids [ 0 .. 29 ] via equality delete - 25 rows removed with 5
                // overlapped from deletion vector commit
                .equalityDelete(
                        ImmutableList.of("widget"),
                        r -> r.get(0, Integer.class) < 30,
                        equalityIds(PRODUCTS_SCHEMA, "product_id"))
                .commit()
                // add 200 rows to gizmo partition
                .append(ImmutableList.of("gizmo"), this::generateProductsRecord, 1, 200)
                .commit()
                // delete all products with color 'green' via equality delete - 40 rows removed
                .equalityDelete(
                        ImmutableList.of("widget", "gizmo"),
                        r -> r.get(3, String.class).equals("green"),
                        equalityIds(PRODUCTS_SCHEMA, "color"))
                .commit()
                // add 200 rows each to widget, gadget, and gizmo partitions
                .append(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        this::generateProductsRecord,
                        1,
                        200)
                .commit()
                .deletionVectors(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                    r -> r.get(0, Integer.class) % 200 >= 100 && r.get(0, Integer.class) > 400)
                .commit();
    }

    /**
     * Creates a 'products_with_deletion_vectors' table with 3 partitions on the category column: widget,
     * gadget, gizmo. 5 data files with 200 rows each are created - 2 in widget, 2 in gizmo, 1 in
     * gadget. Each data file has two row groups, each with 100 rows.
     *
     * <p>
     *
     * <p>Creation steps:
     *
     * <p>
     *
     * <pre>
     * 1. Insert 200 rows with category 'widget', product_ids from [ 0 .. 199 ].            Total rows: 200
     * 2. Delete product_ids [ 0 .. 39 ] via deletion vector                                Total rows: 160
     * 3. Insert 200 rows with category 'gizmo', product_ids from [ 200 .. 399 ].           Total rows: 360
     * 4. Delete gizmo products with color 'green' via deletion vector.                     Total rows: 340
     * 5. Insert 200 rows each to widget, gadget, and gizmo partitions,
     *    product_ids from [ 400 .. 599 ], [ 600 .. 799 ], [ 800 .. 999 ].                  Total rows: 940
     * 6. Delete product ids [ 500 .. 599 ], [ 700 .. 799 ], [ 900 .. 999 ] via DV          Total rows: 640
     *
     * Total rows inserted: 1000
     * Total rows deleted : 360 (via DVs)
     * Final row count    : 640
     * </pre>
     */
    private void createProductsWithDeletionVectorsOnly() throws IOException {
        // Create a properties map with format version 3 for deletion vectors
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(TableProperties.FORMAT_VERSION, "2");
        tableProperties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1));

        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath, conf, TableIdentifier.of("products_with_deletion_vectors"));
        tableGenerator
                .create(
                        PRODUCTS_SCHEMA,
                        PartitionSpec.builderFor(PRODUCTS_SCHEMA).identity("category").build(),
                        tableProperties)
                // add 200 rows to widget partition
                .append(ImmutableList.of("widget"), this::generateProductsRecord, 1, 200)
                .commit();

        // Update to format version 3 to enable deletion vectors
        tableGenerator.updateTablePropertiesToV3();

        tableGenerator
                // delete product_ids [ 0 .. 39 via deletion vector -  40 rows removed
                .deletionVectors(ImmutableList.of("widget"), r -> r.get(0, Integer.class) < 40)
                .commit()
                // add 200 rows to gizmo partition
                .append(ImmutableList.of("gizmo"), this::generateProductsRecord, 1, 200)
                .commit()
                // delete all products with color 'green' via equality delete - 40 rows removed
                .deletionVectors(
                        ImmutableList.of("gizmo"), r -> r.get(3, String.class).equals("green") && r.get(0, Integer.class) >= 40)
                .commit()
                // add 200 rows each to widget, gadget, and gizmo partitions
                .append(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        this::generateProductsRecord,
                        1,
                        200)
                .commit()
                .deletionVectors(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        r -> r.get(0, Integer.class) % 200 >= 100 && r.get(0, Integer.class) > 400)
                .commit();
    }

    /**
     * Creates a 'products_with_deletion_vectors' table with 3 partitions on the category column: widget,
     * gadget, gizmo. 5 data files with 200 rows each are created - 2 in widget, 2 in gizmo, 1 in
     * gadget. Each data file has two row groups, each with 100 rows.
     *
     * <p>
     *
     * <p>Creation steps:
     *
     * <p>
     *
     * <pre>
     * 1. Insert 200 rows with category 'widget', product_ids from [ 0 .. 199 ].            Total rows: 200
     * 2. Delete product_ids [ 0 .. 39 ] via v2 position delete                             Total rows: 160 (40 rows removed by V2 Pos)
     * 4. Insert 200 rows with category 'gizmo', product_ids from [ 200 .. 399 ].           Total rows: 360 (200 rows added)
     * 5. Delete all gizmo products w color 'green' via v2pos del .                         Total rows: 340 (20 rows removed by V2 Pos, No overlap from previous delete because same format)
     * 6. Insert 200 rows each to widget, gadget, and gizmo partitions,
     *    product_ids from [ 400 .. 599 ], [ 600 .. 799 ], [ 800 .. 999 ].                  Total rows: 940 (600 rows added)
     * 9. Delete product ids [ 500 .. 599 ], [ 700 .. 799 ], [ 900 .. 999 ] via DV          Total rows: 640 (300 rows removed by DV)
     *
     * Total rows inserted: 1000
     * Total rows deleted : 360 (60 via V2 Pos, 300 via DV... both go under total-position-deletes)
     * Final row count    :640
     * </pre>
     */
    private void createProductsWithDeletionVectorsAndV2PositionDeletes() throws IOException {
        // Create a properties map with format version 3 for deletion vectors
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(TableProperties.FORMAT_VERSION, "2");
        tableProperties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1));

        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of(
                                "products_with_deletion_vectors_and_v2_position_deletes"));
        tableGenerator
                .create(
                        PRODUCTS_SCHEMA,
                        PartitionSpec.builderFor(PRODUCTS_SCHEMA).identity("category").build(),
                        tableProperties)
                // add 200 rows to widget partition
                .append(ImmutableList.of("widget"), this::generateProductsRecord, 1, 200)
                .commit();

        tableGenerator
                // delete product_ids [ 0 .. 39 via deletion vector -  40 rows removed
                .positionalDelete(ImmutableList.of("widget"), r -> r.get(0, Integer.class) < 40)
                .commit();

        // add 200 rows to gizmo partition
        tableGenerator
                .append(ImmutableList.of("gizmo"), this::generateProductsRecord, 1, 200)
                .commit()
                .positionalDelete(
                        ImmutableList.of("gizmo"), r -> r.get(3, String.class).equals("green"))
                .commit();

        // Update to format version 3 to enable deletion vectors
        tableGenerator.updateTablePropertiesToV3();

        tableGenerator
                // add 200 rows each to widget, gadget, and gizmo partitions
                .append(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        this::generateProductsRecord,
                        1,
                        200)
                .commit()
                .deletionVectors(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        r -> r.get(0, Integer.class) % 200 >= 100 && r.get(0, Integer.class) > 400)
                .commit();
    }

    /**
     * Creates a 'products_with_deletion_vectors' table with 3 partitions on the category column: widget,
     * gadget, gizmo. 5 data files with 200 rows each are created - 2 in widget, 2 in gizmo, 1 in
     * gadget. Each data file has two row groups, each with 100 rows.
     *
     * <p>
     *
     * <p>Creation steps:
     *
     * <p>
     *
     * <pre>
     * 1. Insert 200 rows with category 'widget', product_ids from [ 0 .. 199 ].            Total rows: 200
     * 2. Delete product_ids [ 0 .. 39 ] via v2 position delete                             Total rows: 160 (40 rows removed by V2 Pos)
     * 3. Insert 200 rows with category 'gizmo', product_ids from [ 200 .. 399 ].           Total rows: 360 (200 rows added)
     * 4. Delete all products with color 'green' and via v2 pos delete.                     Total rows: 325 (20 rows removed by Equality Delete, 20 overlap)
     * 5. Insert 200 rows each to widget, gadget, and gizmo partitions,
     *    product_ids from [ 400 .. 599 ], [ 600 .. 799 ], [ 800 .. 999 ].                  Total rows: 925 (600 rows added)
     * 6. Delete product ids [ 500 .. 599 ], [ 700 .. 799 ], [ 900 .. 999 ] via DV          Total rows: 625 (300 rows removed by DVs)
     *
     * Total rows inserted: 1000
     * Total rows deleted : 375 (40 via v2 Position Deletes, 35 via Equality Deletes, 300 via Deletion Vectors)
     * Final row count    : 625
     * </pre>
     */
    private void createProductsWithDeletionVectorsEqualityDeletesAndV2PositionDeletes()
            throws IOException {
        // Create a properties map with format version 3 for deletion vectors
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(TableProperties.FORMAT_VERSION, "2");
        tableProperties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1));

        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of(
                                "products_with_deletion_vectors_and_equality_deletes_and_v2_position_deletes"));
        tableGenerator
                .create(
                        PRODUCTS_SCHEMA,
                        PartitionSpec.builderFor(PRODUCTS_SCHEMA).identity("category").build(),
                        tableProperties)
                // add 200 rows to widget partition
                .append(ImmutableList.of("widget"), this::generateProductsRecord, 1, 200)
                .commit();

        tableGenerator
                // delete product_ids [ 0 .. 39 via deletion vector -  40 rows removed
                .positionalDelete(ImmutableList.of("widget"), r -> r.get(0, Integer.class) < 40)
                .commit();

        tableGenerator
                .append(ImmutableList.of("gizmo"), this::generateProductsRecord, 1, 200)
                .commit()
                .equalityDelete(
                        ImmutableList.of("widget", "gizmo"),
                        r -> r.get(3, String.class).equals("green"),
                        equalityIds(PRODUCTS_SCHEMA, "color"));

        // Update to format version 3 to enable deletion vectors
        tableGenerator.updateTablePropertiesToV3();

        tableGenerator
                // add 200 rows each to widget, gadget, and gizmo partitions
                .append(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        this::generateProductsRecord,
                        1,
                        200)
                .commit()
                .deletionVectors(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        r -> r.get(0, Integer.class) % 200 >= 100 && r.get(0, Integer.class) > 400)
                .commit();
    }

    private void createProductsWithEqDeletesAndPosDeletesSameSequenceNumber() throws IOException {
        IcebergTableGenerator tableGenerator =
                new IcebergTableGenerator(
                        warehousePath,
                        conf,
                        TableIdentifier.of(
                                "products_with_eq_deletes_and_pos_deletes_matching_sequence_numbers"));
        tableGenerator
                .create(
                        PRODUCTS_SCHEMA,
                        PartitionSpec.builderFor(PRODUCTS_SCHEMA).identity("category").build(),
                        ImmutableMap.of(
                                // Iceberg will write at minimum 100 rows per rowgroup, so set row
                                // group size small
                                // enough to
                                // guarantee that happens
                                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1)))
                // add 200 rows to widget partition
                .append(ImmutableList.of("widget"), this::generateProductsRecord, 1, 200)
                .commit()
                // add 200 rows to gizmo partition
                .append(ImmutableList.of("gizmo"), this::generateProductsRecord, 1, 200)
                .commit()
                // delete all products with color 'green' via equality delete - 40 rows removed
                .equalityDelete(
                        ImmutableList.of("widget", "gizmo"),
                        r -> r.get(3, String.class).equals("green"),
                        equalityIds(PRODUCTS_SCHEMA, "color"))
                .commit();

        RowDelta rowDelta = tableGenerator.getTransaction().newRowDelta();
        tableGenerator
                // delete product ids [ 100 .. 199 ], [ 300 .. 399 ].
                // The EQ Deletes should NOT be applied to [ 500 .. 599 ], [ 700 .. 799 ], [ 900 ..
                // 999 ]
                // taking into account previous deletions this deletes 455 rows with 25 overlapped
                // from pos del commit
                .appendWithRowDelta(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        this::generateProductsRecord,
                        1,
                        200,
                        rowDelta,
                        false)
                /*        .positionalDelete(
                ImmutableList.of("widget"),
                r -> r.get(0, Integer.class) >= 250 && r.get(0, Integer.class) < 275, rowDelta)*/
                .equalityDelete(
                        ImmutableList.of("widget", "gadget", "gizmo"),
                        r -> r.get(0, Integer.class) % 200 >= 100,
                        equalityIds(PRODUCTS_SCHEMA, "product_id"),
                        rowDelta)
                .commit();
    }

    private GenericRecord getFakeOrdersRecordForExtraDeletes() {
        GenericRecord record = GenericRecord.create(ORDERS_SCHEMA);
        record.set(0, 0);
        record.set(1, 0);
        record.set(2, LocalDateTime.now());
        record.set(3, 0);
        record.set(4, "");
        record.set(5, 0.0);
        return record;
    }

    private GenericRecord generateProductsRecord(ValueGenerator generator, String category) {
        GenericRecord record = GenericRecord.create(PRODUCTS_SCHEMA);
        int id = generator.id();
        String name =
                String.format(
                        generator.select(PRODUCT_NAME_TEMPLATES), StringUtils.capitalize(category));
        String suffix = generator.select(PRODUCT_SUFFIXES);
        if (!suffix.isEmpty()) {
            name = name + " " + suffix;
        }

        record.set(0, id);
        record.set(1, name);
        record.set(2, category);
        record.set(3, COLORS.get(id % COLORS.size()));
        record.set(4, LocalDate.of(2022 - (id / 12), 12 - (id % 12), 1));
        record.set(5, generator.doubleRange(0.1, 50.0));
        record.set(6, generator.intRange(0, 10000));
        return record;
    }

    private RecordGenerator<String> createProductsRecordGenerator(
            IcebergTableGenerator tableGenerator) {
        return (generator, category) -> {
            GenericRecord record = GenericRecord.create(tableGenerator.getTable().schema());
            int id = generator.id();
            String name =
                    String.format(
                            generator.select(PRODUCT_NAME_TEMPLATES),
                            StringUtils.capitalize(category));
            String suffix = generator.select(PRODUCT_SUFFIXES);
            if (!suffix.isEmpty()) {
                name = name + " " + suffix;
            }

            for (int i = 0; i < record.size(); i++) {
                Object value = null;
                switch (record.struct().fields().get(i).name()) {
                    case "product_id":
                        value = id;
                        break;
                    case "name":
                        value = name;
                        break;
                    case "category":
                        value = category;
                        break;
                    case "color":
                        value = COLORS.get(id % COLORS.size());
                        break;
                    case "created_date":
                        value = LocalDate.of(2022 - (id / 12), 12 - (id % 12), 1);
                        break;
                    case "weight":
                        value = generator.doubleRange(0.1, 50.0);
                        break;
                    case "quantity":
                        value = generator.intRange(0, 10000);
                        break;
                }

                record.set(i, value);
            }

            return record;
        };
    }

    private List<Integer> equalityIds(Schema schema, String... fields) {
        return Arrays.stream(fields)
                .map(f -> schema.findField(f).fieldId())
                .collect(Collectors.toList());
    }

    private GenericRecord generateWideMetricsRecord(ValueGenerator generator, Void unused) {
        GenericRecord record = GenericRecord.create(WIDE_METRICS_SCHEMA);
        record.set(0, generator.id());
        for (int i = 1; i < WIDE_METRICS_N_COLS; i++) {
            record.set(i, generator.doubleRange(0, 100));
        }
        return record;
    }
}