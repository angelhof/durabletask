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
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    /// <summary>
    /// Abstractions for the back-end, including transport and partition management.
    /// </summary>
    internal static class Backend
    {
        /// <summary>
        /// Back-end, as seen by the host.
        /// </summary>
        public interface ITaskHub
        {
            Task<bool> ExistsAsync();

            Task CreateAsync();

            Task DeleteAsync();

            Task StartAsync();

            Task StopAsync();
        }

        /// <summary>
        /// The host, as seen by the back-end.
        /// </summary>
        public interface IHost
        {
            IClient AddClient(Guid clientId, ISender<PartitionEvent> batchSender);

            IPartition AddPartition(uint partitionId, ISender<Event> batchSender);
        }

        /// <summary>
        /// A sender abstraction, passed to clients and partitions, for sending messages
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public interface ISender<in T>
        {
            void Submit(T element);

            void Submit(IEnumerable<T> batch);
        }

        /// <summary>
        /// A client, as seen by the back-end.
        /// </summary>
        public interface IClient
        {
            Guid ClientId { get; }

            void Process(IEnumerable<ClientEvent> batch);

            void ConfirmDurablySent(IEnumerable<PartitionEvent> sent);
        }

        /// <summary>
        /// A partition, as seen by the back-end.
        /// </summary>
        public interface IPartition
        {
            uint PartitionId { get; }

            Task<long> StartAsync();

            Task ProcessAsync(IEnumerable<PartitionEvent> batch);

            Task TakeCheckpoint(long position);

            void ConfirmDurablySent(IEnumerable<Event> sent);

            Task StopAsync();
        }
    }
}