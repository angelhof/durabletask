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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal class PubSub<K,V>
    {
        private readonly Dictionary<K, List<Listener>> listeners = new Dictionary<K, List<Listener>>();

        public PubSub(CancellationToken token)
        {
            token.Register(this.CancelAll);
        }

        public void CancelAll()
        {
            lock (listeners)
            {
                List<Listener> toRemove = new List<Listener>();

                foreach (var kvp in listeners)
                {
                    foreach (var promise in kvp.Value)
                    {
                        toRemove.Add(promise);
                    }
                }

                listeners.Clear(); // to prevent concurrent modification exceptions

                foreach(var promise in toRemove)
                {
                    promise.TryCancel();
                }
            }
        }

        public void Notify(K key, V value)
        {
            lock(listeners)
            {
                if (listeners.TryGetValue(key, out var list))
                {
                    listeners.Remove(key); // to avoid concurrent modification exceptions

                    List<Listener> keep = null; // we'll add back the ones to keep

                    foreach (var listener in list)
                    {
                        listener.Notify(value);

                        if (!listener.Task.IsCompleted)
                        {
                            (keep ?? (keep = new List<Listener>())).Add(listener);
                        }
                    }

                    if (keep != null)
                    {
                        listeners[key] = keep;
                    }
                }
            }
        }  

        public abstract class Listener : CancellableCompletionSource<V>
        {
            public Listener(CancellationToken token) : base(token) { }

            public abstract void Notify(V value);

            private PubSub<K, V> subscribedAt;

            protected K Key { get; private set; }

            public void Subscribe(PubSub<K, V> pubsub, K key)
            {
                if (this.subscribedAt != null)
                {
                    throw new InvalidOperationException();
                }

                this.subscribedAt = pubsub;
                this.Key = key;

                lock (pubsub.listeners)
                {
                    if (!pubsub.listeners.TryGetValue(key, out var list))
                    {
                        pubsub.listeners[key] = list = new List<Listener>();
                    }
                    list.Add(this);
                }
            }

            protected override void Cleanup()
            {
                base.Cleanup();

                if (subscribedAt != null)
                {
                    lock (subscribedAt.listeners)
                    {
                        if (subscribedAt.listeners.TryGetValue(this.Key, out var list))
                        {
                            list.Remove(this);

                            if (list.Count == 0)
                            {
                                subscribedAt.listeners.Remove(this.Key);
                            }
                        }                  
                    }
                }
            }
        }
    }
}
