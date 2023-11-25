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
package io.trino.operator.join.unspilled;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.ExceededMemoryLimitException;
import io.trino.RowPagesBuilder;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.operator.DriverContext;
import io.trino.operator.JoinOperatorType;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.ProcessorContext;
import io.trino.operator.TaskContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorFactory;
import io.trino.operator.join.InternalJoinFilterFunction;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinOperatorInfo;
import io.trino.operator.join.unspilled.JoinTestUtils.BuildSideSetup;
import io.trino.operator.join.unspilled.JoinTestUtils.TestInternalJoinFilterFunction;
import io.trino.spi.Page;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.DataProviders;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import io.trino.util.FinalizerService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.operator.JoinOperatorType.fullOuterJoin;
import static io.trino.operator.JoinOperatorType.innerJoin;
import static io.trino.operator.JoinOperatorType.probeOuterJoin;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.dropChannel;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.operator.OperatorFactories.join;
import static io.trino.operator.WorkProcessor.ProcessState.finished;
import static io.trino.operator.WorkProcessor.ProcessState.ofResult;
import static io.trino.operator.join.unspilled.JoinTestUtils.buildLookupSource;
import static io.trino.operator.join.unspilled.JoinTestUtils.getHashChannelAsInt;
import static io.trino.operator.join.unspilled.JoinTestUtils.innerJoinOperatorFactory;
import static io.trino.operator.join.unspilled.JoinTestUtils.instantiateBuildDrivers;
import static io.trino.operator.join.unspilled.JoinTestUtils.setupBuildSide;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.trueFalse;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestHashJoinOperator
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private NodePartitioningManager nodePartitioningManager;

    @BeforeMethod
    public void setUp()
    {
        // Before/AfterMethod is chosen here because the executor needs to be shutdown
        // after every single test case to terminate outstanding threads, if any.

        // The line below is the same as newCachedThreadPool(daemonThreadsNamed(...)) except RejectionExecutionHandler.
        // RejectionExecutionHandler is set to DiscardPolicy (instead of the default AbortPolicy) here.
        // Otherwise, a large number of RejectedExecutionException will flood logging, resulting in Travis failure.
        executor = new ThreadPoolExecutor(
                0,
                Integer.MAX_VALUE,
                60L,
                SECONDS,
                new SynchronousQueue<>(),
                daemonThreadsNamed("test-executor-%s"),
                new ThreadPoolExecutor.DiscardPolicy());
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(
                new InMemoryNodeManager(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(new FinalizerService())));
        nodePartitioningManager = new NodePartitioningManager(
                nodeScheduler,
                TYPE_OPERATORS,
                CatalogServiceProvider.fail());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testInnerJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT));
        List<Page> probeInput = probePages
                .addSequencePage(1000, 0, 1000, 2000)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypesWithoutHash(), buildPages.getTypesWithoutHash()))
                .row("20", 1020L, 2020L, "20", 30L, 40L)
                .row("21", 1021L, 2021L, "21", 31L, 41L)
                .row("22", 1022L, 2022L, "22", 32L, 42L)
                .row("23", 1023L, 2023L, "23", 33L, 43L)
                .row("24", 1024L, 2024L, "24", 34L, 44L)
                .row("25", 1025L, 2025L, "25", 35L, 45L)
                .row("26", 1026L, 2026L, "26", 36L, 46L)
                .row("27", 1027L, 2027L, "27", 37L, 47L)
                .row("28", 1028L, 2028L, "28", 38L, 48L)
                .row("29", 1029L, 2029L, "29", 39L, 49L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinRleProbeTestValues")
    public void testInnerJoinWithRunLengthEncodedProbe(boolean withFilter, boolean probeHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(false, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT))
                .row("20", 1L)
                .row("21", 2L)
                .row("21", 3L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, false, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePagesBuilder = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT))
                .addBlocksPage(
                        RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("20"), 2),
                        createLongsBlock(42, 43))
                .addBlocksPage(
                        RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("-1"), 2),
                        createLongsBlock(52, 53))
                .addBlocksPage(
                        RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("21"), 2),
                        createLongsBlock(62, 63));
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePagesBuilder, withFilter);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        List<Page> pages = toPages(joinOperatorFactory, driverContext, probePagesBuilder.build(), true, true);
        if (probeHashEnabled) {
            // Drop the hashChannel for all pages
            pages = dropChannel(pages, getHashChannels(probePagesBuilder, buildPages));
        }

        assertThat(pages.size()).isEqualTo(2);
        if (withFilter) {
            assertThat(pages.get(0).getBlock(2)).isInstanceOf(VariableWidthBlock.class);
            assertThat(pages.get(0).getBlock(3)).isInstanceOf(LongArrayBlock.class);
        }
        else {
            assertThat(pages.get(0).getBlock(2)).isInstanceOf(RunLengthEncodedBlock.class);
            assertThat(pages.get(0).getBlock(3)).isInstanceOf(RunLengthEncodedBlock.class);
        }
        assertThat(pages.get(1).getBlock(2)).isInstanceOf(VariableWidthBlock.class);

        assertThat(getJoinOperatorInfo(driverContext).getRleProbes()).isEqualTo(withFilter ? 0 : 2);
        assertThat(getJoinOperatorInfo(driverContext).getTotalProbes()).isEqualTo(3);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePagesBuilder.getTypesWithoutHash(), buildPages.getTypesWithoutHash()))
                .row("20", 42L, "20", 1L)
                .row("20", 43L, "20", 1L)
                .row("21", 62L, "21", 3L)
                .row("21", 62L, "21", 2L)
                .row("21", 63L, "21", 3L)
                .row("21", 63L, "21", 2L)
                .build();
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    private JoinOperatorInfo getJoinOperatorInfo(DriverContext driverContext)
    {
        return (JoinOperatorInfo) getOnlyElement(driverContext.getOperatorStats()).getInfo();
    }

    @DataProvider(name = "hashJoinRleProbeTestValues")
    public static Object[][] hashJoinRleProbeTestValuesProvider()
    {
        return cartesianProduct(trueFalse(), trueFalse(), trueFalse());
    }

    @Test(dataProvider = "singleBigintLookupSourceProvider")
    public void testUnwrapsLazyBlocks(boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftPage, rightPosition, rightPage) -> {
                    // force loading of probe block
                    rightPage.getBlock(1).getLoadedBlock();
                    return true;
                }));

        RowPagesBuilder buildPages = rowPagesBuilder(false, Ints.asList(0), ImmutableList.of(BIGINT)).addSequencePage(1, 0);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, true, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        RowPagesBuilder probePages = rowPagesBuilder(false, Ints.asList(0), ImmutableList.of(BIGINT, BIGINT));
        List<Page> probeInput = probePages.addSequencePage(1, 0, 0).build();
        probeInput = probeInput.stream()
                .map(page -> new Page(page.getBlock(0), new LazyBlock(1, () -> page.getBlock(1))))
                .collect(toImmutableList());

        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                true,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(driverContext);
        assertThat(operator.needsInput()).isTrue();
        operator.addInput(probeInput.get(0));
        operator.finish();

        Page output = operator.getOutput();
        assertThat(output.getBlock(1) instanceof LazyBlock).isFalse();
    }

    @Test(dataProvider = "singleBigintLookupSourceProvider")
    public void testYield(boolean singleBigintLookupSource)
    {
        // create a filter function that yields for every probe match
        // verify we will yield #match times totally

        TaskContext taskContext = createTaskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        // force a yield for every match
        AtomicInteger filterFunctionCalls = new AtomicInteger();
        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftPage, rightPosition, rightPage) -> {
                    filterFunctionCalls.incrementAndGet();
                    driverContext.getYieldSignal().forceYieldForTesting();
                    return true;
                }));

        // build with 40 entries
        int entries = 40;
        RowPagesBuilder buildPages = rowPagesBuilder(true, Ints.asList(0), ImmutableList.of(BIGINT))
                .addSequencePage(entries, 42);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, true, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe matching the above 40 entries
        RowPagesBuilder probePages = rowPagesBuilder(false, Ints.asList(0), ImmutableList.of(BIGINT));
        List<Page> probeInput = probePages.addSequencePage(100, 0).build();
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                true,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(driverContext);
        assertThat(operator.needsInput()).isTrue();
        operator.addInput(probeInput.get(0));
        operator.finish();

        // we will yield 40 times due to filterFunction
        for (int i = 0; i < entries; i++) {
            driverContext.getYieldSignal().setWithDelay(5 * SECONDS.toNanos(1), driverContext.getYieldExecutor());
            filterFunctionCalls.set(0);
            assertThat(operator.getOutput()).isNull();
            assertThat(filterFunctionCalls.get())
                    .describedAs("Expected join to stop processing (yield) after calling filter function once")
                    .isEqualTo(1);
            driverContext.getYieldSignal().reset();
        }
        // delayed yield is not going to prevent operator from producing a page now (yield won't be forced because filter function won't be called anymore)
        driverContext.getYieldSignal().setWithDelay(5 * SECONDS.toNanos(1), driverContext.getYieldExecutor());
        // expect output page to be produced within few calls to getOutput(), e.g. to facilitate spill
        Page output = null;
        for (int i = 0; output == null && i < 5; i++) {
            output = operator.getOutput();
        }
        assertThat(output).isNotNull();
        driverContext.getYieldSignal().reset();

        // make sure we have all 4 entries
        assertThat(output.getPositionCount()).isEqualTo(entries);
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testInnerJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testInnerJoinWithOutputSingleMatch(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();
        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, true, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testInnerJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testInnerJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testProbeOuterJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("20", 1020L, 2020L, "20", 30L, 40L)
                .row("21", 1021L, 2021L, "21", 31L, 41L)
                .row("22", 1022L, 2022L, "22", 32L, 42L)
                .row("23", 1023L, 2023L, "23", 33L, 43L)
                .row("24", 1024L, 2024L, "24", 34L, 44L)
                .row("25", 1025L, 2025L, "25", 35L, 45L)
                .row("26", 1026L, 2026L, "26", 36L, 46L)
                .row("27", 1027L, 2027L, "27", 37L, 47L)
                .row("28", 1028L, 2028L, "28", 38L, 48L)
                .row("29", 1029L, 2029L, "29", 39L, 49L)
                .row("30", 1030L, 2030L, null, null, null)
                .row("31", 1031L, 2031L, null, null, null)
                .row("32", 1032L, 2032L, null, null, null)
                .row("33", 1033L, 2033L, null, null, null)
                .row("34", 1034L, 2034L, null, null, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testProbeOuterJoinWithFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftPage, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(1), rightPosition) >= 1025));

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.of(filterFunction));
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, true);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("20", 1020L, 2020L, null, null, null)
                .row("21", 1021L, 2021L, null, null, null)
                .row("22", 1022L, 2022L, null, null, null)
                .row("23", 1023L, 2023L, null, null, null)
                .row("24", 1024L, 2024L, null, null, null)
                .row("25", 1025L, 2025L, "25", 35L, 45L)
                .row("26", 1026L, 2026L, "26", 36L, 46L)
                .row("27", 1027L, 2027L, "27", 37L, 47L)
                .row("28", 1028L, 2028L, "28", 38L, 48L)
                .row("29", 1029L, 2029L, "29", 39L, 49L)
                .row("30", 1030L, 2030L, null, null, null)
                .row("31", 1031L, 2031L, null, null, null)
                .row("32", 1032L, 2032L, null, null, null)
                .row("33", 1033L, 2033L, null, null, null)
                .row("34", 1034L, 2034L, null, null, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testOuterJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(null, null)
                .row(null, null)
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testOuterJoinWithNullProbeAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftPage, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(0), rightPosition) == 1L));

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, true);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(null, null)
                .row(null, null)
                .row(1L, 1L)
                .row(2L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testOuterJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(3L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testOuterJoinWithNullBuildAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftPage, rightPosition, rightPage) ->
                        ImmutableSet.of(1L, 3L).contains(BIGINT.getLong(rightPage.getBlock(0), rightPosition))));

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, true);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, null)
                .row(3L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testOuterJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(null, null)
                .row(3L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testOuterJoinWithNullOnBothSidesAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftPage, rightPosition, rightPage) ->
                        ImmutableSet.of(1L, 3L).contains(BIGINT.getLong(rightPage.getBlock(0), rightPosition))));

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, true);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, null)
                .row(null, null)
                .row(3L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "testMemoryLimitProvider")
    public void testMemoryLimit(boolean parallelBuild, boolean buildHashEnabled)
    {
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.ofBytes(100));

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        instantiateBuildDrivers(buildSideSetup, taskContext);

        assertThatThrownBy(() -> buildLookupSource(executor, buildSideSetup))
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessageMatching("Query exceeded per-node memory limit of.*");
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testInnerJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

        // drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(0, true, true, false).addDriverContext());

        List<Page> pages = probePages.row(1L).build();
        operator.addInput(pages.get(0));
        Page outputPage = operator.getOutput();
        assertThat(outputPage).isNull();
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testLookupOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = join(
                JoinOperatorType.lookupOuterJoin(false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

        // drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(0, true, true, false).addDriverContext());

        List<Page> pages = probePages.row(1L).build();
        operator.addInput(pages.get(0));
        Page outputPage = operator.getOutput();
        assertThat(outputPage).isNull();
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testProbeOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = join(
                probeOuterJoin(false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, null)
                .row(2L, null)
                .row(null, null)
                .row(3L, null)
                .build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testFullOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = join(
                fullOuterJoin(),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, null)
                .row(2L, null)
                .row(null, null)
                .row(3L, null)
                .build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public void testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages.build();
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes)).build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testInnerJoinWithBlockingLookupSourceAndEmptyProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        // join that waits for build side to be collected
        TaskContext taskContext = createTaskContext();
        OperatorFactory joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, probeHashEnabled, buildHashEnabled, true);
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        try (Operator joinOperator = joinOperatorFactory.createOperator(driverContext)) {
            joinOperatorFactory.noMoreOperators();
            assertThat(joinOperator.needsInput()).isFalse();
            joinOperator.finish();
            assertThat(joinOperator.getOutput()).isNull();

            // lookup join operator got blocked waiting for build side
            assertThat(joinOperator.isBlocked().isDone()).isFalse();
            assertThat(joinOperator.isFinished()).isFalse();
        }

        // join that doesn't wait for build side to be collected
        taskContext = createTaskContext();
        joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, probeHashEnabled, buildHashEnabled, false);
        driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        try (Operator joinOperator = joinOperatorFactory.createOperator(driverContext)) {
            joinOperatorFactory.noMoreOperators();
            assertThat(joinOperator.needsInput()).isTrue();
            joinOperator.finish();
            assertThat(joinOperator.getOutput()).isNull();

            // lookup join operator will yield once before finishing
            assertThat(joinOperator.getOutput()).isNull();
            assertThat(joinOperator.isBlocked().isDone()).isTrue();
            assertThat(joinOperator.isFinished()).isTrue();
        }
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testInnerJoinWithBlockingLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR));
        Page probePage = getOnlyElement(probePages.addSequencePage(1, 0).build());

        // join that waits for build side to be collected
        TaskContext taskContext = createTaskContext();
        OperatorFactory joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, probeHashEnabled, buildHashEnabled, true);
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        try (Operator joinOperator = joinOperatorFactory.createOperator(driverContext)) {
            joinOperatorFactory.noMoreOperators();
            assertThat(joinOperator.needsInput()).isFalse();
            assertThat(joinOperator.getOutput()).isNull();

            // lookup join operator got blocked waiting for build side
            assertThat(joinOperator.isBlocked().isDone()).isFalse();
            assertThat(joinOperator.isFinished()).isFalse();
        }

        // join that doesn't wait for build side to be collected
        taskContext = createTaskContext();
        joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, probeHashEnabled, buildHashEnabled, false);
        driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        try (Operator joinOperator = joinOperatorFactory.createOperator(driverContext)) {
            joinOperatorFactory.noMoreOperators();
            assertThat(joinOperator.needsInput()).isTrue();
            assertThat(joinOperator.getOutput()).isNull();

            // join needs input page
            assertThat(joinOperator.isBlocked().isDone()).isTrue();
            assertThat(joinOperator.isFinished()).isFalse();
            joinOperator.addInput(probePage);
            assertThat(joinOperator.getOutput()).isNull();

            // lookup join operator got blocked waiting for build side
            assertThat(joinOperator.isBlocked().isDone()).isFalse();
            assertThat(joinOperator.isFinished()).isFalse();
        }
    }

    @Test
    public void testInnerJoinLoadsPagesInOrder()
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(false, Ints.asList(0), buildTypes);
        for (int i = 0; i < 100_000; ++i) {
            buildPages.row("a");
        }
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, false, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, INTEGER, INTEGER);
        RowPagesBuilder probePages = rowPagesBuilder(false, Ints.asList(0), probeTypes);
        probePages.row("a", 1L, 2L);
        WorkProcessorOperatorFactory joinOperatorFactory = (WorkProcessorOperatorFactory) innerJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        Page probePage = getOnlyElement(probePages.build());
        AtomicInteger totalProbePages = new AtomicInteger();
        WorkProcessor<Page> inputPages = WorkProcessor.create(() -> {
            int probePageNumber = totalProbePages.incrementAndGet();
            if (probePageNumber == 5) {
                return finished();
            }

            return ofResult(new Page(
                    1,
                    probePage.getBlock(0),
                    // this block should not be loaded by join operator as it's not being used by join
                    new LazyBlock(1, () -> probePage.getBlock(1)),
                    // this block is force loaded in test to ensure join doesn't fetch next probe page before joining
                    // and outputting current probe page
                    new LazyBlock(
                            1,
                            () -> {
                                // when loaded this block should be the latest one
                                assertThat(probePageNumber).isEqualTo(totalProbePages.get());
                                return probePage.getBlock(2);
                            })));
        });

        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(joinOperatorFactory.getOperatorId(), joinOperatorFactory.getPlanNodeId(), joinOperatorFactory.getOperatorType());
        WorkProcessorOperator joinOperator = joinOperatorFactory.create(
                new ProcessorContext(taskContext.getSession(), taskContext.getTaskMemoryContext(), operatorContext),
                inputPages);
        WorkProcessor<Page> outputPages = joinOperator.getOutputPages();
        int totalOutputPages = 0;
        for (int i = 0; i < 1_000_000; ++i) {
            if (!outputPages.process()) {
                // allow join to progress
                driverContext.getYieldSignal().resetYieldForTesting();
                continue;
            }

            if (outputPages.isFinished()) {
                break;
            }

            Page page = outputPages.getResult();
            totalOutputPages++;
            assertThat(page.getBlock(1).isLoaded()).isFalse();
            page.getBlock(2).getLoadedBlock();

            // yield to enforce more complex execution
            driverContext.getYieldSignal().forceYieldForTesting();
        }

        // make sure that multiple pages were produced for some probe pages
        assertThat(totalOutputPages > totalProbePages.get()).isTrue();
        assertThat(outputPages.isFinished()).isTrue();
    }

    private OperatorFactory createJoinOperatorFactoryWithBlockingLookupSource(TaskContext taskContext, boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean waitForBuild)
    {
        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, waitForBuild),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);

        return joinOperatorFactory;
    }

    @DataProvider(name = "hashJoinTestValues")
    public static Object[][] hashJoinTestValuesProvider()
    {
        return DataProviders.cartesianProduct(
                new Object[][] {{true}, {false}},
                new Object[][] {{true}, {false}},
                new Object[][] {{true}, {false}});
    }

    @DataProvider
    public static Object[][] testMemoryLimitProvider()
    {
        return DataProviders.cartesianProduct(
                new Object[][] {{true}, {false}},
                new Object[][] {{true}, {false}});
    }

    @DataProvider(name = "singleBigintLookupSourceProvider")
    public static Object[][] singleBigintLookupSourceProvider()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider(name = "hashJoinTestValuesAndsingleBigintLookupSourceProvider")
    public static Object[][] hashJoinTestValuesAndsingleBigintLookupSourceProvider()
    {
        return DataProviders.cartesianProduct(
                new Object[][] {{true}, {false}},
                new Object[][] {{true}, {false}},
                new Object[][] {{true}, {false}},
                new Object[][] {{true}, {false}});
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }

    private static List<Integer> getHashChannels(RowPagesBuilder probe, RowPagesBuilder build)
    {
        ImmutableList.Builder<Integer> hashChannels = ImmutableList.builder();
        if (probe.getHashChannel().isPresent()) {
            hashChannels.add(probe.getHashChannel().get());
        }
        if (build.getHashChannel().isPresent()) {
            hashChannels.add(probe.getTypes().size() + build.getHashChannel().get());
        }
        return hashChannels.build();
    }

    private OperatorFactory probeOuterJoinOperatorFactory(
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
            RowPagesBuilder probePages,
            boolean hasFilter)
    {
        return join(
                probeOuterJoin(false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                hasFilter,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);
    }

    private static <T> List<T> concat(List<T> initialElements, List<T> moreElements)
    {
        return ImmutableList.copyOf(Iterables.concat(initialElements, moreElements));
    }
}
