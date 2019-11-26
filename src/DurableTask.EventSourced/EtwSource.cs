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
using System.Diagnostics.Tracing;
using System.Linq;
using System.Threading;
using DurableTask.Core;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// ETW Event Provider for the DurableTask.EventSourced provider extension.
    /// </summary>
    /// <remarks>
    /// The ETW Provider ID for this event source is {b3b94da0-1edd-53a7-435e-53129d278be4}.
    /// </remarks>
    [EventSource(Name = "DurableTask-EventSourced")]
    class EtwSource : EventSource
    {
        /// <summary>
        /// Singleton instance used for writing events.
        /// </summary>
        public static readonly EtwSource Log = new EtwSource();

        // global trace emission switches, useful for experimentation and profiling
#if DEBUG
        public static bool EmitEtwTrace => true;
        public static bool EmitDiagnosticsTrace => true;
#else
        public static bool EmitEtwTrace => true;
        public static bool EmitDiagnosticsTrace => false;
#endif

        // we should always check if verbose is enabled before doing extensive string formatting for a verbose event
        public bool IsVerboseEnabled => EmitEtwTrace && this.IsEnabled();

        // ----- starting/stopping of the host

        // we are grouping all events on this host using a single activity id
        // and since we are only using one host per machine, we can save its id in this static field.
        private static Guid hostId; 

        [Event(10, Level = EventLevel.Informational, Opcode = EventOpcode.Start, Version = 1)]
        public void HostStarted(Guid hostId, string machineName)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(10, hostId, machineName);
            EtwSource.hostId = hostId;
        }

        [Event(11, Level = EventLevel.Informational, Opcode = EventOpcode.Stop, Version = 1)]
        public void HostStopped(Guid hostId)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(11, hostId);
        }

        // ----- events observed on a partition processor

        [Event(20, Level = EventLevel.Informational, Version = 1)]
        public void PartitionStarted(uint partitionId)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(20, partitionId);
        }

        [Event(21, Level = EventLevel.Informational, Version = 1)]
        public void PartitionStopped(uint partitionId)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(21, partitionId);
        }

        [Event(22, Level = EventLevel.Error, Version = 1)]
        public void PartitionErrorReported(uint partitionId, string where, string exceptionType, string message)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(22, partitionId, where, message);
        }

        [Event(30, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionEventReceived(uint partitionId, string context, string workItem, string eventInfo)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(30, partitionId, context, workItem, eventInfo);
        }

        [Event(31, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionEventSent(uint partitionId, string context, string workItem, string eventInfo)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(31, partitionId, context, workItem, eventInfo);
        }

        [Event(32, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionWorkItemEnqueued(uint partitionId, string context, string workItem)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(32, partitionId, context, workItem);
        }

        // -----  events observed on a client

        [Event(50, Level = EventLevel.Informational, Version = 1)]
        public void ClientStarted(Guid clientId)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(50, clientId);
        }

        [Event(51, Level = EventLevel.Informational, Version = 1)]
        public void ClientStopped(Guid clientId)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(51, clientId);
        }

        [Event(52, Level = EventLevel.Error, Version = 1)]
        public void ClientErrorReported(Guid clientId, string where, string exceptionType, string message)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(52, clientId, where, message);
        }

        [Event(60, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventReceived(Guid clientId, string eventInfo)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(60, clientId, eventInfo);
        }

        [Event(61, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventSent(Guid clientId, string eventInfo)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(61, clientId, eventInfo);
        }
    }
}
