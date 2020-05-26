//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.EventSourced.Faster;
using Microsoft.Azure.Storage;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace DurableTask.EventSourced.Tests
{
    [Collection("EventSourcedTests")]
    public class FasterTests
    {
        private readonly ILoggerFactory loggerFactory;

        public FasterTests(ITestOutputHelper outputHelper)
        {
            loggerFactory = new LoggerFactory();
            var loggerProvider = new XunitLoggerProvider(outputHelper);
            loggerFactory.AddProvider(loggerProvider);
        }

        [Theory]
        [InlineData(false, 3, 5)]
        [InlineData(false, 30, 50)]
        [InlineData(false, 300, 500)]
        [InlineData(true, 1, 10)]
        [InlineData(true, 30, 50)]
        [InlineData(true, 300, 500)]
        public async Task RecoverLog(bool useAzure, int numEntries, int maxBytesPerEntry)
        {
            List<byte[]> entries = new List<byte[]>();
            List<long> positions = new List<long>();

            var random = new Random(0);

            var taskHubName = useAzure ? "test-taskhub" : Guid.NewGuid().ToString("N");
            var account = useAzure ? CloudStorageAccount.Parse(TestHelpers.GetAzureStorageConnectionString()) : null;
            var logger = loggerFactory.CreateLogger("testlogger");

            await BlobManager.DeleteTaskhubStorageAsync(account, taskHubName);

            // first, commit some number of random entries to the log and record the commit positions
            {
                var blobManager = new BlobManager(
                    account, 
                    taskHubName, 
                    logger, 
                    Microsoft.Extensions.Logging.LogLevel.Trace, 
                    0, 
                    new PartitionErrorHandler(0, logger, Microsoft.Extensions.Logging.LogLevel.Trace, "account", taskHubName));

                await blobManager.StartAsync();
                var log = new DurableTask.EventSourced.Faster.FasterLog(blobManager);

                for (int i = 0; i < numEntries; i++)
                {
                    var bytes = new Byte[1 + random.Next(maxBytesPerEntry)];
                    random.NextBytes(bytes);
                    entries.Add(bytes);
                    positions.Add(log.Enqueue(entries[i]));
                }
                await log.CommitAsync();

                await blobManager.StopAsync();
            }

            // then, read back all the entries, and compare position and content
            {
                var blobManager = new BlobManager(
                    account, 
                    taskHubName, 
                    logger, 
                    Microsoft.Extensions.Logging.LogLevel.Trace, 
                    0, 
                    new PartitionErrorHandler(0, logger, Microsoft.Extensions.Logging.LogLevel.Trace, "account", taskHubName));

                await blobManager.StartAsync();
                var log = new DurableTask.EventSourced.Faster.FasterLog(blobManager);

                int iterationCount = 0;
                await Iterate(0, positions[positions.Count - 1]);

                async Task Iterate(long from, long to)
                {
                    using (var iter = log.Scan(from, to + 1))
                    {
                        byte[] result;
                        int entryLength;
                        long currentAddress;

                        while (true)
                        {
                            var next = iter.NextAddress;

                            while (!iter.GetNext(out result, out entryLength, out currentAddress))
                            {
                                if (currentAddress >= to)
                                {
                                    Assert.Equal(iterationCount, numEntries);
                                    return;
                                }
                                await iter.WaitAsync();
                            }

                            // process entry
                            Assert.Equal(positions[iterationCount], next);
                            var reference = entries[iterationCount];
                            Assert.Equal(reference.Length, entryLength);
                            for (int i = 0; i < entryLength; i++)
                            {
                                Assert.Equal(reference[i], result[i]);
                            }

                            iterationCount++;
                        }
                    }
                }

                await blobManager.StopAsync();
            }
        }


        ///// <summary>
        ///// This tests the slow read path when a read misses memory 
        ///// so the IO is executed asynchronously and the read is completed at a later time.
        ///// </summary>
        ///// <param name="useAzure"></param>
        ///// <param name="numEntries"></param>
        ///// <param name="maxBytesPerEntry"></param>
        ///// <returns></returns>
        //[Theory]
        //[InlineData(false, 3, 5)]
        //public async Task ReadMemoryMiss(bool useAzure, int numEntries, int maxBytesPerEntry)
        //{
        //    // Would it make sense to run this test for different configurations?

        //    // Configuration
        //    var taskHubName = useAzure ? "test-taskhub" : Guid.NewGuid().ToString("N");
        //    var account = useAzure ? CloudStorageAccount.Parse(TestHelpers.GetAzureStorageConnectionString()) : null;
        //    var logger = loggerFactory.CreateLogger("testlogger");
        //    uint partitionId = 0;

        //    // Delete any existing stored data from previous tests
        //    await BlobManager.DeleteTaskhubStorageAsync(account, taskHubName);

        //    // Start a BlobManager
        //    var blobManager = new BlobManager(
        //            account,
        //            taskHubName,
        //            logger,
        //            Microsoft.Extensions.Logging.LogLevel.Trace,
        //            partitionId,
        //            new PartitionErrorHandler(0, logger, Microsoft.Extensions.Logging.LogLevel.Trace, "account", taskHubName));

        //    await blobManager.StartAsync();

        //    // Q: Do I need to initialize something on a higher level, say a StoreWorker?
        //    //var fasterKV = new DurableTask.EventSourced.Faster.FasterKV(partitionId, blobManager);

        //    // Start the Counter
        //    int initialValue = 0;
        //    var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), initialValue);

        //    // Need to wait for the instance to start before sending events to it.
        //    // TODO: This requirement may not be ideal and should be revisited.
        //    await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

        //    // Perform some operations
        //    await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpIncrement);
        //    await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpIncrement);
        //    await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpIncrement);
        //    await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpDecrement);
        //    await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpIncrement);
        //    await Task.Delay(2000);

        //    // Make sure it's still running and didn't complete early (or fail).
        //    var status = await client.GetStatusAsync();
        //    Assert.True(
        //        status?.OrchestrationStatus == OrchestrationStatus.Running ||
        //        status?.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

        //    // The end message will cause the actor to complete itself.
        //    await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpEnd);

        //    status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

        //    Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        //    Assert.Equal(3, JToken.Parse(status?.Output));

        //    // When using ContinueAsNew, the original input is discarded and replaced with the most recent state.
        //    Assert.NotEqual(initialValue, JToken.Parse(status?.Input));

        //    // Flush the circular buffer that serves as the in-memory FASTER cache to 
        //    // evict the history (or state?) of the entity

        //    // Perform another method of the entity, ensuring that the returning value is correct
        //    List<byte[]> entries = new List<byte[]>();
        //    List<long> positions = new List<long>();

        //    var random = new Random(0);

            

            

        //    // first, commit some number of random entries to the log and record the commit positions
        //    {
                

        //        for (int i = 0; i < numEntries; i++)
        //        {
        //            var bytes = new Byte[1 + random.Next(maxBytesPerEntry)];
        //            random.NextBytes(bytes);
        //            entries.Add(bytes);
        //            positions.Add(log.Enqueue(entries[i]));
        //        }
        //        await log.CommitAsync();

        //        await blobManager.StopAsync();
        //    }

        //    // then, read back all the entries, and compare position and content
        //    {
        //        var blobManager = new BlobManager(
        //            account,
        //            taskHubName,
        //            logger,
        //            Microsoft.Extensions.Logging.LogLevel.Trace,
        //            0,
        //            new PartitionErrorHandler(0, logger, Microsoft.Extensions.Logging.LogLevel.Trace, "account", taskHubName));

        //        await blobManager.StartAsync();
        //        var log = new DurableTask.EventSourced.Faster.FasterLog(blobManager);

        //        int iterationCount = 0;
        //        await Iterate(0, positions[positions.Count - 1]);

        //        async Task Iterate(long from, long to)
        //        {
        //            using (var iter = log.Scan(from, to + 1))
        //            {
        //                byte[] result;
        //                int entryLength;
        //                long currentAddress;

        //                while (true)
        //                {
        //                    var next = iter.NextAddress;

        //                    while (!iter.GetNext(out result, out entryLength, out currentAddress))
        //                    {
        //                        if (currentAddress >= to)
        //                        {
        //                            Assert.Equal(iterationCount, numEntries);
        //                            return;
        //                        }
        //                        await iter.WaitAsync();
        //                    }

        //                    // process entry
        //                    Assert.Equal(positions[iterationCount], next);
        //                    var reference = entries[iterationCount];
        //                    Assert.Equal(reference.Length, entryLength);
        //                    for (int i = 0; i < entryLength; i++)
        //                    {
        //                        Assert.Equal(reference[i], result[i]);
        //                    }

        //                    iterationCount++;
        //                }
        //            }
        //        }

        //        await blobManager.StopAsync();
        //    }
        //}

        // This is copied from PortedAzureScenarioTests. Q: Is there a better way to do it?
        internal class Counter : TaskOrchestration<int, int>
        {
            TaskCompletionSource<string> waitForOperationHandle;
            internal const string OpEventName = "operation";
            internal const string OpIncrement = "incr";
            internal const string OpDecrement = "decr";
            internal const string OpEnd = "end";

            public override async Task<int> RunTask(OrchestrationContext context, int currentValue)
            {
                string operation = await this.WaitForOperation();

                bool done = false;
                switch (operation?.ToLowerInvariant())
                {
                    case OpIncrement:
                        currentValue++;
                        break;
                    case OpDecrement:
                        currentValue--;
                        break;
                    case OpEnd:
                        done = true;
                        break;
                }

                if (!done)
                {
                    context.ContinueAsNew(currentValue);
                }
                return currentValue;
            }

            async Task<string> WaitForOperation()
            {
                this.waitForOperationHandle = new TaskCompletionSource<string>();
                string operation = await this.waitForOperationHandle.Task;
                this.waitForOperationHandle = null;
                return operation;
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                Assert.Equal(OpEventName, name);
                if (this.waitForOperationHandle != null)
                {
                    this.waitForOperationHandle.SetResult(input);
                }
            }
        }
    }
}
