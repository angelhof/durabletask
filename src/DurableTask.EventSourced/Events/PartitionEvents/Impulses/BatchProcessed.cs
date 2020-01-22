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
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class BatchProcessed : PartitionEvent
    {
        [DataMember]
        public long SessionId { get; set; }

        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public long BatchStartPosition { get; set; }

        [DataMember]
        public int BatchLength { get; set; }

        [DataMember]
        public List<HistoryEvent> NewEvents { get; set; }

        [DataMember]
        public OrchestrationState State { get; set; }

        [DataMember]
        public List<TaskMessage> ActivityMessages { get; set; }

        [DataMember]
        public List<TaskMessage> LocalMessages { get; set; }

        [DataMember]
        public List<TaskMessage> RemoteMessages { get; set; }

        [DataMember]
        public List<TaskMessage> TimerMessages { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [IgnoreDataMember]
        public OrchestrationRuntimeState InMemoryRuntimeState { get; set; }

        public override void DetermineEffects(TrackedObject.EffectList effects)
        {
            // lists all the objects on which this event is processed.
            // Effects are applied in reverse order listed, i.e. the
            // history state is updated first, and the sessions state last.

            effects.Add(TrackedObjectKey.Sessions);

            if (this.ActivityMessages?.Count > 0)
            {
                effects.Add(TrackedObjectKey.Activities);
            }

            if (this.TimerMessages?.Count > 0)
            {
                effects.Add(TrackedObjectKey.Timers);
            }

            if (this.RemoteMessages?.Count > 0)
            {
                effects.Add(TrackedObjectKey.Outbox);
            }

            if (this.State != null)
            {      
                effects.Add(TrackedObjectKey.Instance(this.InstanceId));
                effects.Add(TrackedObjectKey.History(this.InstanceId));
            }
        }

        protected override void TraceInformation(StringBuilder s)
        {
            base.TraceInformation(s);

            if (State != null)
            {
                s.Append(' ');
                s.Append(State.OrchestrationStatus);
            }

            s.Append(' ');
            s.Append(this.InstanceId);
        }

        [IgnoreDataMember]
        public override string WorkItem => $"S{SessionId:D6}:{BatchStartPosition}[{BatchLength}]";

    }

}
