﻿//  ----------------------------------------------------------------------------------
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
using DurableTask.EventSourced.Faster;
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
        [InlineData(false, 300, 5000)]
        [InlineData(true, 1, 10)]
        [InlineData(true, 30, 50)]
        [InlineData(true, 300, 5000)]
        public async Task RecoverLog(bool useAzure, int numEntries, int maxBytesPerEntry)
        {
            List<byte[]> entries = new List<byte[]>();
            List<long> positions = new List<long>();

            var random = new Random(0);

            var taskHubName = useAzure ? "test-taskhub" : Guid.NewGuid().ToString("N");
            var connectionString = TestHelpers.GetAzureStorageConnectionString();

            BlobManager.SetLocalFileDirectoryForTestingAndDebugging(!useAzure);
            var blobManager = new BlobManager(connectionString, taskHubName, loggerFactory.CreateLogger("faster"), 0);

            await BlobManager.DeleteTaskhubStorageAsync(useAzure ? connectionString : null, taskHubName);
            await blobManager.StartAsync();
            await blobManager.AcquireOwnership(CancellationToken.None);

            // first, commit some number of random entries to the log and record the commit positions
            using (var log = new DurableTask.EventSourced.Faster.FasterLog(blobManager))
            {
                for (int i = 0; i < numEntries; i++)
                {
                    var bytes = new Byte[1 + random.Next(maxBytesPerEntry)];
                    random.NextBytes(bytes);
                    entries.Add(bytes);
                    positions.Add(log.Enqueue(entries[i]));
                }
                await log.CommitAsync();
            }

            // then, read back all the entries, and compare position and content
            int iterationCount = 0;
            using (var log = new DurableTask.EventSourced.Faster.FasterLog(blobManager))
            {
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
            }

            // shut down the blob manager so the leases get released
            await blobManager.StopAsync();
        }
    }
}

