/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.opentelemetry.api.common.Attributes;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_SIZE;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

@Execution(ExecutionMode.SAME_THREAD)
public class TestDeltaLakeAlluxioCacheFileOperations
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Path cacheDirectory = Files.createTempDirectory("cache");
        cacheDirectory.toFile().deleteOnExit();
        Path metastoreDirectory = Files.createTempDirectory(DELTA_CATALOG);
        metastoreDirectory.toFile().deleteOnExit();

        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema("default")
                .build();

        Map<String, String> deltaLakeProperties = ImmutableMap.<String, String>builder()
                .put("fs.cache.enabled", "true")
                .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                .put("fs.cache.max-sizes", "100MB")
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", metastoreDirectory.toUri().toString())
                .put("delta.enable-non-concurrent-writes", "true")
                .buildOrThrow();

        DistributedQueryRunner queryRunner = DeltaLakeQueryRunner.builder(session)
                .setCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                .setDeltaProperties(deltaLakeProperties)
                .setCatalogName(DELTA_CATALOG)
                .setNodeCount(2)
                .build();

        queryRunner.execute("CREATE SCHEMA " + session.getSchema().orElseThrow());
        return queryRunner;
    }

    @Test
    public void testCacheFileOperations()
    {
        assertUpdate("DROP TABLE IF EXISTS test_cache_file_operations");
        assertUpdate("CREATE TABLE test_cache_file_operations(key varchar, data varchar) with (partitioned_by=ARRAY['key'])");
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p1', '1-abc')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p2', '2-xyz')", 1);
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_cache_file_operations')");
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p2/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p2/", 0, 218))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218))
                        .build());
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p3', '3-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p4', '4-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p5', '5-xyz')", 1);
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p3/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p4/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p5/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p3/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p4/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p5/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p3/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p4/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p5/", 0, 218))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p3/", 0, 218), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p4/", 0, 218), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p5/", 0, 218), 1)
                        .build());
    }

    @Test
    public void testCacheCheckpointFileOperations()
    {
        assertUpdate("DROP TABLE IF EXISTS test_checkpoint_file_operations");
        assertUpdate("CREATE TABLE test_checkpoint_file_operations(key varchar, data varchar) with (checkpoint_interval = 2, partitioned_by=ARRAY['key'])");
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p1', '1-abc')", 1);
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p2', '2-xyz')", 1);
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_checkpoint_file_operations')");
        assertFileSystemAccesses(
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p2/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p2/", 0, 218))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218))
                        .build());
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p3', '3-xyz')", 1);
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p4', '4-xyz')", 1);
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p5', '5-xyz')", 1);
        assertFileSystemAccesses(
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p3/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p4/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p5/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p3/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p4/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p5/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p3/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p4/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p5/", 0, 218))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p3/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p4/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p5/", 0, 218))
                        .build());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<CacheOperation> expectedCacheAccesses)
    {
        assertUpdate("CALL system.flush_metadata_cache()");
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(getCacheOperations(), expectedCacheAccesses);
    }

    private Multiset<CacheOperation> getCacheOperations()
    {
        return getQueryRunner().getSpans().stream()
                .filter(span -> span.getName().startsWith("Alluxio."))
                .filter(span -> !isTrinoSchemaOrPermissions(requireNonNull(span.getAttributes().get(CACHE_FILE_LOCATION))))
                .map(span -> CacheOperation.create(span.getName(), span.getAttributes()))
                .collect(toCollection(HashMultiset::create));
    }

    private static Pattern dataFilePattern = Pattern.compile(".*?/(?<partition>key=[^/]*/)?(?<queryId>\\d{8}_\\d{6}_\\d{5}_\\w{5})_(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    private record CacheOperation(String operationName, String fileId, long position, long length)
    {
        public static CacheOperation create(String operationName, Attributes attributes)
        {
            String path = requireNonNull(attributes.get(CACHE_FILE_LOCATION));
            String fileName = path.replaceFirst(".*/", "");

            long position = switch (operationName) {
                case "Alluxio.readCached" -> requireNonNull(attributes.get(CACHE_FILE_READ_POSITION));
                case "Alluxio.readExternal" -> requireNonNull(attributes.get(CACHE_FILE_READ_POSITION));
                case "Alluxio.writeCache" -> requireNonNull(attributes.get(CACHE_FILE_WRITE_POSITION));
                default -> throw new IllegalArgumentException("Unexpected operation name: " + operationName);
            };

            long length = switch (operationName) {
                case "Alluxio.readCached" -> requireNonNull(attributes.get(CACHE_FILE_READ_SIZE));
                case "Alluxio.readExternal" -> requireNonNull(attributes.get(CACHE_FILE_READ_SIZE));
                case "Alluxio.writeCache" -> requireNonNull(attributes.get(CACHE_FILE_WRITE_SIZE));
                default -> throw new IllegalArgumentException("Unexpected operation name: " + operationName);
            };

            if (!path.contains("_delta_log") && !path.contains("/.trino")) {
                Matcher matcher = dataFilePattern.matcher(path);
                if (matcher.matches()) {
                    return new CacheOperation(operationName, matcher.group("partition"), position, length);
                }
            }
            else {
                return new CacheOperation(operationName, fileName, position, length);
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }
    }

    private static boolean isTrinoSchemaOrPermissions(String path)
    {
        return path.endsWith(".trinoSchema") || path.contains(".trinoPermissions");
    }
}
