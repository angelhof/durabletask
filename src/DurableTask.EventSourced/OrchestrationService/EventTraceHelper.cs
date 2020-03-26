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

using DurableTask.Core;
using DurableTask.Core.History;
using Dynamitey;
using Dynamitey.DynamicObjects;
using FASTER.core;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    internal class EventTraceHelper
    {
        private readonly ILogger logger;
        private readonly string account;
        private readonly string taskHub;
        private readonly int partitionId;

        public EventTraceHelper(ILogger logger, Partition partition)
        {
            this.logger = logger;
            this.account = partition.StorageAccountName;
            this.taskHub = partition.Settings.TaskHubName;
            this.partitionId = (int)partition.PartitionId;
        }

        public bool IsTracingDetails => (this.logger.IsEnabled(LogLevel.Debug) || EtwSource.Log.IsVerboseEnabled);

        public void TraceEventProcessingStarted(long commitLogPosition, PartitionEvent evt, bool replaying)
        {
            System.Diagnostics.Debug.Assert(IsTracingDetails);
            long nextCommitLogPosition = ((evt as PartitionUpdateEvent)?.NextCommitLogPosition) ?? 0L;
            var details = string.Format($"{(replaying ? "Replaying" : "Processing")} {(evt.NextInputQueuePosition > 0 ? "external" : "internal")} {(nextCommitLogPosition > 0 ? "update" : "read")} event {evt} id={evt.EventIdString}");
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                this.logger.LogDebug("Part{partition:D2}.{commitLogPosition:D10} {details}", this.partitionId, commitLogPosition, details);
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventDetail(this.account, this.taskHub, this.partitionId, commitLogPosition, evt.EventIdString ?? "", details, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceEventProcessingDetail(string details)
        {
            System.Diagnostics.Debug.Assert(IsTracingDetails);
            (long commitLogPosition, string eventId) = EventTraceContext.Current;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} {details}", this.partitionId, prefix, details);
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventDetail(this.account, this.taskHub, this.partitionId, commitLogPosition, eventId ?? "", details, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceEventProcessed(long commitLogPosition, PartitionEvent evt, double startedTimestamp, double finishedTimestamp, bool replaying)
        {
            long nextCommitLogPosition = ((evt as PartitionUpdateEvent)?.NextCommitLogPosition) ?? 0L;
            double queueLatencyMs = evt.IssuedTimestamp - evt.ReceivedTimestamp;
            double fetchLatencyMs = startedTimestamp - evt.IssuedTimestamp;
            double latencyMs = finishedTimestamp - startedTimestamp;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                var details = string.Format($"{(replaying ? "Replayed" : "Processed")} {(evt.NextInputQueuePosition > 0 ? "external" : "internal")} {(nextCommitLogPosition > 0 ? "update" : "read")} event");
                this.logger.LogDebug("Part{partition:D2}.{commitLogPosition:D10} {details} {event} id={eventId} pos=({nextCommitLogPosition},{nextInputQueuePosition}) latency=({queueLatencyMs:F1},{fetchLatencyMs:F3},{latencyMs:F3})", this.partitionId, commitLogPosition, details, evt, evt.EventIdString, nextCommitLogPosition, evt.NextInputQueuePosition, queueLatencyMs, fetchLatencyMs, latencyMs);
            }

            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventProcessed(this.account, this.taskHub, this.partitionId, commitLogPosition, evt.EventIdString, evt.ToString(), nextCommitLogPosition, evt.NextInputQueuePosition, queueLatencyMs, fetchLatencyMs, latencyMs, replaying, TraceUtils.ExtensionVersion);
            }
        }

        public void TracePartitionOffloadDecision(int reportedLocalLoad, int pending, int backlog, int remotes, string reportedRemotes)
        {
            (long commitLogPosition, string eventId) = EventTraceContext.Current;

            string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
            this.logger.LogInformation("Part{partition:D2}{prefix} Offload decision reportedLocalLoad={reportedLocalLoad} pending={pending} backlog={backlog} remotes={remotes} reportedRemotes={reportedRemotes}",
                this.partitionId, prefix, reportedLocalLoad, pending, backlog, remotes, reportedRemotes);

            EtwSource.Log.PartitionOffloadDecision(this.account, this.taskHub, this.partitionId, commitLogPosition, eventId, reportedLocalLoad, pending, backlog, remotes, reportedRemotes, TraceUtils.ExtensionVersion);
        }

        public void TraceTaskMessageReceived(TaskMessage message, string sessionPosition)
        {
            (long commitLogPosition, string eventId) = EventTraceContext.Current;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} received TaskMessage eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId} messageId={messageId} sessionPosition={SessionPosition}",
                    this.partitionId, prefix, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId, eventId, sessionPosition);
            }

            EtwSource.Log.TaskMessageReceived(this.account, this.taskHub, this.partitionId, commitLogPosition, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", eventId, sessionPosition, TraceUtils.ExtensionVersion);
        }

        public void TraceTaskMessageSent(TaskMessage message, string sentEventId)
        {
            (long commitLogPosition, string eventId) = EventTraceContext.Current;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} sent TaskMessage eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId} messageId={messageId}",
                    this.partitionId, prefix, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId, sentEventId);
            }

            EtwSource.Log.TaskMessageSent(this.account, this.taskHub, this.partitionId, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", sentEventId, TraceUtils.ExtensionVersion);
        }

        public void TraceTaskMessageDiscarded(TaskMessage message, string reason, string workItemId)
        {
            (long commitLogPosition, string eventId) = EventTraceContext.Current;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} discarded TaskMessage reason={reason} eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId} workItemId={workItemId}",
                    this.partitionId, prefix, reason, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId, workItemId);
            }

            EtwSource.Log.TaskMessageDiscarded(this.account, this.taskHub, this.partitionId, reason, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", workItemId, TraceUtils.ExtensionVersion);
        }


        public void TraceOrchestrationWorkItemQueued(OrchestrationWorkItem workItem)
        {
            (long commitLogPosition, string eventId) = EventTraceContext.Current;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} Queued {executionType} OrchestrationWorkItem {workItemId} instanceId={instanceId} ",
                    this.partitionId, prefix, workItem.Type, workItem.MessageBatch.WorkItemId, workItem.InstanceId);
            }

            EtwSource.Log.OrchestrationWorkItemQueued(this.account, this.taskHub, this.partitionId, workItem.Type.ToString(), workItem.InstanceId, workItem.MessageBatch.WorkItemId, TraceUtils.ExtensionVersion);
        }

        public void TraceOrchestrationWorkItemDiscarded(BatchProcessed evt)
        {
            (long commitLogPosition, string eventId) = EventTraceContext.Current;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} Discarded OrchestrationWorkItem workItemId={workItemId} instanceId={instanceId}",
                    this.partitionId, prefix, evt.WorkItemId, evt.InstanceId);
            }

            EtwSource.Log.OrchestrationWorkItemDiscarded(this.account, this.taskHub, this.partitionId, evt.InstanceId, evt.WorkItemId, TraceUtils.ExtensionVersion);
        }


        public void TraceInstanceUpdate(string workItemId, string instanceId, string executionId, int totalEventCount, List<HistoryEvent> newEvents, int episode)
        {
            string eventNames = string.Empty;
            string eventType = string.Empty;
            int numNewEvents = 0;

            if (newEvents != null)
            {
                HistoryEvent orchestratorEvent = null;
                var eventNamesBuilder = new StringBuilder();
                numNewEvents = newEvents.Count;

                if (numNewEvents > 20)
                {
                    eventNamesBuilder.Append("...,");
                }

                for (int i = 0; i < numNewEvents; i++)
                {
                    var historyEvent = newEvents[i];
                    switch (historyEvent.EventType)
                    {
                        case EventType.ExecutionStarted:
                        case EventType.ExecutionCompleted:
                        case EventType.ExecutionTerminated:
                        case EventType.ContinueAsNew:
                            orchestratorEvent = historyEvent;
                            break;
                        default:
                            break;
                    }
                    if (i >= newEvents.Count - 20)
                    {
                        eventNamesBuilder.Append(historyEvent.EventType.ToString());
                        eventNamesBuilder.Append(",");
                    }
                }
                eventNames = eventNamesBuilder.ToString(0, eventNamesBuilder.Length - 1); // remove trailing comma
                if (orchestratorEvent != null)
                {
                    eventType = orchestratorEvent.EventType.ToString();
                }
            }

            (long commitLogPosition, string eventId) = EventTraceContext.Current;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} Updated history instanceId={instanceId} executionId={executionId} workItemId={workItemId} numNewEvents={numNewEvents} totalEventCount={totalEventCount} eventNames={eventNames} eventType={eventType} episode={episode}",
                    this.partitionId, prefix, instanceId, executionId, workItemId, numNewEvents, totalEventCount, eventNames, eventType, episode);
            }

            EtwSource.Log.InstanceUpdated(this.account, this.taskHub, instanceId, executionId, workItemId, numNewEvents, totalEventCount, eventNames, eventType, episode, TraceUtils.ExtensionVersion);
        }

        public void TraceInstanceFetch(string workItemId, string instanceId, string executionId, bool history, bool state)
        { }

             public void FetchedInstanceHistory(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            int EventCount,
            int Episode,
            int RequestCount,
            long LatencyMs,
            string ETag,
            DateTime LastCheckpointTime,
            string ExtensionVersion)
        { }
        public void FetchedInstanceStatus(
           string Account,
           string TaskHub,
           string InstanceId,
           string ExecutionId,
           long LatencyMs,
           string ExtensionVersion)
        { }
    }
}