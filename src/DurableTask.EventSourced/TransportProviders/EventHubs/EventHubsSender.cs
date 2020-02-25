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

using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
//using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventHubsSender<T> : BatchWorker<Event> where T: Event
    {
        private readonly PartitionSender sender;
        private readonly TransportAbstraction.IHost host;
        private readonly ILogger logger;

        public EventHubsSender(TransportAbstraction.IHost host, PartitionSender sender)
        {
            this.host = host;
            this.sender = sender;
            this.logger = host.TransportLogger;
        }
   
        // we reuse the same memory stream t
        private readonly MemoryStream stream = new MemoryStream();

        private TimeSpan backoff = TimeSpan.FromSeconds(5);

        protected override async Task Process(IList<Event> toSend)
        {
            if (toSend.Count == 0)
            {
                return;
            }

            // track progress in case of exception
            var sentSuccessfully = 0;
            var maybeSent = 0;
            Exception senderException = null;

            try
            {
                var batch = sender.CreateBatch();

                for (int i = 0; i < toSend.Count; i++)
                {
                    var startpos = (int)stream.Position;

                    Serializer.SerializeEvent(toSend[i], stream);

                    var arraySegment = new ArraySegment<byte>(stream.GetBuffer(), startpos, (int)stream.Position - startpos);
                    var eventData = new EventData(arraySegment);

                    if (batch.TryAdd(eventData))
                    {
                        maybeSent = i;
                        continue;
                    }
                    else if (batch.Count > 0)
                    {
                        // send the batch we have so far, then create a new batch
                        await sender.SendAsync(batch);
                        sentSuccessfully = i;
                        batch = sender.CreateBatch();
                        stream.Seek(0, SeekOrigin.Begin);
                        // backtrack one so we try to send this same element again
                        i--;
                    }
                    else
                    {
                        // the message is too big. Break it into fragments, and send each individually.
                        var fragments = FragmentationAndReassembly.Fragment(arraySegment, toSend[i]);
                        maybeSent = i;
                        foreach (var fragment in fragments)
                        {
                            stream.Seek(0, SeekOrigin.Begin);
                            Serializer.SerializeEvent((Event)fragment, stream);
                            await sender.SendAsync(new EventData(new ArraySegment<byte>(stream.GetBuffer(), 0, (int)stream.Position)));
                        }
                        sentSuccessfully = i + 1;
                    }
                }

                if (batch.Count > 0)
                {
                    await sender.SendAsync(batch);
                }
                sentSuccessfully = toSend.Count;
            }
            catch (Exception e)
            {
                this.logger.LogWarning(e, "Could not send messages to EventHub {eventHubName}, partition {partitionId}", this.sender.EventHubClient.EventHubName, this.sender.PartitionId);
                senderException = e;
            }

            // Confirm all sent ones, and retry or report maybe-sent ones
            List<Event> requeue = null;

            try
            {
                int confirmed = 0;
                int requeued = 0;
                int dropped = 0;

                for (int i = 0; i < toSend.Count; i++)
                {
                    var evt = toSend[i];

                    if (i < sentSuccessfully)
                    {
                        // the event was definitely sent successfully
                        AckListeners.Acknowledge(evt);
                        confirmed++;
                    }
                    else if (i > maybeSent || evt.SafeToDuplicateInTransport())
                    {
                        // the event was definitely not sent, OR it was maybe sent but can be duplicated safely
                        (requeue ?? (requeue = new List<Event>())).Add(evt);
                        requeued++;
                    }
                    else 
                    {
                        // the event may have been sent or maybe not, report problem to listener
                        // this is used by clients who can give the exception back to the caller
                        AckListeners.ReportException(evt, senderException);
                        dropped++;
                    }
                }

                if (requeue != null)
                {
                    // take a deep breath before trying again
                    await Task.Delay(backoff);

                    this.Requeue(requeue);
                }

                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("Confirmed {confirmed}, Requeued {requeued}, Dropped {dropped} messages for EventHub {eventHubName}, partition {partitionId}", confirmed, requeued, dropped, this.sender.EventHubClient.EventHubName, this.sender.PartitionId);
                }

            }
            catch (Exception e)
            {
                this.logger.LogError(e, "Internal error while trying to confirm messages for EventHub {eventHubName}, partition {partitionId}", this.sender.EventHubClient.EventHubName, this.sender.PartitionId);
            }
        }

        private IEnumerable<EventData> Serialize(IEnumerable<Event> events)
        {
            using (var stream = new MemoryStream())
            {
                foreach (var evt in events)
                {
                    stream.Seek(0, SeekOrigin.Begin);
                    Serializer.SerializeEvent(evt, stream);
                    var length = (int)stream.Position;
                    yield return new EventData(new ArraySegment<byte>(stream.GetBuffer(), 0, length));
                }
            }
        }
    }
}
