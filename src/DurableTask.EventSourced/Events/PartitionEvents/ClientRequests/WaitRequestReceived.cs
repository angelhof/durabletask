﻿//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

using DurableTask.Core;
using System;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class WaitRequestReceived : ClientRequestEvent
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public string ExecutionId { get; set; }

        [IgnoreDataMember]
        public override bool AtMostOnce => false;

        [IgnoreDataMember]
        public override bool PersistInLog => false;

        protected override void TraceInformation(StringBuilder s)
        {
            s.Append(' ');
            s.Append(this.InstanceId);
        }

        public override void DetermineEffects(TrackedObject.EffectList effects)
        {
            effects.Partition.Assert(!effects.InRecovery);
            _ = WaitForOrchestrationCompletionTask(effects.Partition);
        }

        public async Task WaitForOrchestrationCompletionTask(Partition partition)
        {
            try
            {
                var waiter = new OrchestrationWaiter(this, partition);

                // start an async read from state
                partition.State.ScheduleRead(waiter);

                var response = await waiter.Task;

                if (response != null)
                {
                    partition.Send(response);
                }
            }
            catch (TaskCanceledException)
            {
            }
            catch (Exception e)
            {
                partition.ReportError($"{nameof(WaitRequestReceived)}.{nameof(WaitForOrchestrationCompletionTask)}", e);
            }
        }

        private class OrchestrationWaiter : 
            Partition.ResponseWaiter, 
            PubSub<string, OrchestrationState>.IListener,
            StorageAbstraction.IReadContinuation
        {
            public OrchestrationWaiter(WaitRequestReceived request, Partition partition)
                : base(partition.PartitionShutdownToken, request, partition)
            {
                Key = request.InstanceId;
                partition.InstanceStatePubSub.Subscribe(this);
            }

            public string Key { get; private set; }

            public TrackedObjectKey ReadTarget => TrackedObjectKey.Instance(this.Key);

            public void Notify(OrchestrationState value)
            {
                if (value != null &&
                    value.OrchestrationStatus != OrchestrationStatus.Running &&
                    value.OrchestrationStatus != OrchestrationStatus.Pending &&
                    value.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew)
                {
                    this.TrySetResult(new WaitResponseReceived()
                    {
                        ClientId = Request.ClientId,
                        RequestId = Request.RequestId,
                        OrchestrationState = value
                    });
                }
            }

            public void OnReadComplete(TrackedObject target)
            {
                this.Notify(((InstanceState)target)?.OrchestrationState);
            }

            protected override void Cleanup()
            {
                this.Partition.InstanceStatePubSub.Unsubscribe(this);
                base.Cleanup();
            }
        }
    }
}
