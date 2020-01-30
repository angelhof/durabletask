﻿//  Copyright Microsoft Corporation
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    internal class Partition : TransportAbstraction.IPartition
    {
        private readonly EventSourcedOrchestrationService host;

        public uint PartitionId { get; private set; }
        public Func<string, uint> PartitionFunction { get; private set; }

        public EventSourcedOrchestrationServiceSettings Settings { get; private set; }

        public StorageAbstraction.IPartitionState State { get; private set; }
        public TransportAbstraction.ISender BatchSender { get; private set; }
        public WorkQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        public WorkQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }

        public CancellationToken PartitionShutdownToken => this.partitionShutdown.Token;

        public BatchTimer<PartitionEvent> PendingTimers { get; private set; }
        public PubSub<string, OrchestrationState> InstanceStatePubSub { get; private set; }
        public ConcurrentDictionary<long, ResponseWaiter> PendingResponses { get; private set; }

        private readonly CancellationTokenSource partitionShutdown;

        [ThreadStatic]
        public static string TraceContext;

        public Partition(
            EventSourcedOrchestrationService host,
            uint partitionId,
            Func<string, uint> partitionFunction,
            StorageAbstraction.IPartitionState state,
            TransportAbstraction.ISender batchSender,
            EventSourcedOrchestrationServiceSettings settings,
            WorkQueue<TaskActivityWorkItem> activityWorkItemQueue,
            WorkQueue<TaskOrchestrationWorkItem> orchestrationWorkItemQueue,
            CancellationToken serviceShutdownToken)
        {
            this.host = host;
            this.PartitionId = partitionId;
            this.PartitionFunction = partitionFunction;
            this.State = state;
            this.BatchSender = batchSender;
            this.Settings = settings;
            this.ActivityWorkItemQueue = activityWorkItemQueue;
            this.OrchestrationWorkItemQueue = orchestrationWorkItemQueue;

            this.partitionShutdown = CancellationTokenSource.CreateLinkedTokenSource(
                serviceShutdownToken,
                state.OwnershipCancellationToken);
        }

        public async Task<long> StartAsync()
        {
            // initialize collections for pending work
            this.PendingTimers = new BatchTimer<PartitionEvent>(this.PartitionShutdownToken, this.TimersFired);
            this.InstanceStatePubSub = new PubSub<string, OrchestrationState>();
            this.PendingResponses = new ConcurrentDictionary<long, ResponseWaiter>();

            // restore from last snapshot
            var inputQueuePosition = await State.RestoreAsync(this);

            this.PendingTimers.Start($"Timer{this.PartitionId:D2}");

            return inputQueuePosition;
        }

        public void ProcessAsync(PartitionEvent partitionEvent)
        {
            this.State.Submit(partitionEvent);
        }

        public async Task StopAsync()
        {
            // stop all in-progress activities (timers, work items etc.)
            this.partitionShutdown.Cancel();

            // wait for current state (log and store) to be persisted
            await this.State.PersistAndShutdownAsync();

            EtwSource.Log.PartitionStopped(this.PartitionId);
        }

        private void TimersFired(List<PartitionEvent> timersFired)
        {
            this.SubmitRange(timersFired);
        }

        public class ResponseWaiter : CancellableCompletionSource<ClientEvent>
        {
            protected readonly ClientRequestEvent Request;
            protected readonly Partition Partition;

            public ResponseWaiter(CancellationToken token, ClientRequestEvent request, Partition partition) : base(token)
            {
                this.Request = request;
                this.Partition = partition;
                this.Partition.PendingResponses.TryAdd(Request.RequestId, this);
            }
            protected override void Cleanup()
            {
                this.Partition.PendingResponses.TryRemove(Request.RequestId, out var _);
                base.Cleanup();
            }
        }

        public void TrySendResponse(ClientRequestEvent request, ClientEvent response)
        {
            if (this.PendingResponses.TryGetValue(request.RequestId, out var waiter))
            {
                waiter.TrySetResult(response);
            }
        }

        public void Send(Event evt)
        {
            this.TraceSend(evt);
            this.BatchSender.Submit(evt);
        }

        public void Submit(PartitionEvent evt)
        {
            this.State.Submit(evt);
        }

        public void SubmitRange(IEnumerable<PartitionEvent> partitionEvents)
        {
            this.State.SubmitRange(partitionEvents);
        }

        public void EnqueueActivityWorkItem(ActivityWorkItem item)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                this.DiagnosticsTrace($"Creating ActivityWorkItem {item.WorkItemId}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionWorkItemEnqueued(this.PartitionId, Partition.TraceContext ?? "", item.WorkItemId);
            }

            this.ActivityWorkItemQueue.Add(item);
        }

        public void EnqueueOrchestrationWorkItem(OrchestrationWorkItem item)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                this.DiagnosticsTrace($"Creating OrchestrationWorkItem {item.WorkItemId}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionWorkItemEnqueued(this.PartitionId, Partition.TraceContext ?? "", item.WorkItemId);
            }

            this.OrchestrationWorkItemQueue.Add(item);
        }

        public void ReportError(string where, Exception e)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                System.Diagnostics.Trace.TraceError($"Part{this.PartitionId:D2} !!! Exception in {where}: {e}");
            }
            if (EtwSource.EmitEtwTrace)
            {
                EtwSource.Log.PartitionErrorReported(this.PartitionId, where, e.GetType().Name, e.Message);
            }
        }

        public void TraceProcess(PartitionEvent evt)
        {
            Partition.TraceContext = $"{evt.CommitLogPosition:D10}   ";

            if (EtwSource.EmitDiagnosticsTrace)
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2}.{evt.CommitLogPosition:D10} Processing {evt} {evt.WorkItem}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventReceived(this.PartitionId, Partition.TraceContext ?? "", evt.WorkItem, evt.ToString());
            }
        }

        public void TraceSend(Event evt)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                this.DiagnosticsTrace($"Sending {evt} {evt.WorkItem}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventSent(this.PartitionId, Partition.TraceContext ?? "", evt.WorkItem, evt.ToString());
            }
        }

        public void TraceSubmit(Event evt)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                this.DiagnosticsTrace($"Submitting {evt} {evt.CommitLogPosition:D10} {evt.WorkItem}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventSent(this.PartitionId, Partition.TraceContext ?? "", evt.WorkItem, evt.ToString());
            }
        }

        public void DiagnosticsTrace(string msg)
        {
            var context = Partition.TraceContext;
            if (string.IsNullOrEmpty(context))
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2} {msg}");
            }
            else
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2}.{context} {msg}");
            }
        }

        [Conditional("DEBUG")]
        public void Assert(bool condition)
        {
            if (!condition)
            {
                var stacktrace = System.Environment.StackTrace;

                if (EtwSource.EmitDiagnosticsTrace)
                {
                    System.Diagnostics.Trace.TraceError($"Part{this.PartitionId:D2} !!! Assertion failed {stacktrace}");
                }
 
                System.Diagnostics.Debugger.Break();
            }
        }
    }
}
