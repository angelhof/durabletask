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

namespace DurableTask.EventHubs.Tests
{
    using System;
    using Xunit;

    public class ConfigurationTests
    {
        /// <summary>
        /// Ensures default settings are valid.
        /// </summary>
        [Fact]
        public void ValidateDefaultSettings()
        {
            EventHubsOrchestrationServiceSettings.Validate(
                new EventHubsOrchestrationServiceSettings());
        }

        /// <summary>
        /// Ensures null settings are invalid.
        /// </summary>
        [Fact]
        public void ValidateNullSettings()
        {
            Assert.Throws<ArgumentNullException>(
                () => EventHubsOrchestrationServiceSettings.Validate(null));
        }

        /// <summary>
        /// Ensures settings with non-positive work item concurrencies are invalid.
        /// </summary>
        [Fact]
        public void ValidateNonPositiveWorkItemConcurrency()
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                () => EventHubsOrchestrationServiceSettings.Validate(
                    new EventHubsOrchestrationServiceSettings
                    {
                         MaxConcurrentTaskActivityWorkItems = 0,
                    }));

            Assert.Throws<ArgumentOutOfRangeException>(
                () => EventHubsOrchestrationServiceSettings.Validate(
                    new EventHubsOrchestrationServiceSettings
                    {
                        MaxConcurrentTaskOrchestrationWorkItems = 0,
                    }));

            Assert.Throws<ArgumentOutOfRangeException>(
                () => EventHubsOrchestrationServiceSettings.Validate(
                    new EventHubsOrchestrationServiceSettings
                    {
                        MaxConcurrentTaskActivityWorkItems = -1,
                    }));

            Assert.Throws<ArgumentOutOfRangeException>(
                () => EventHubsOrchestrationServiceSettings.Validate(
                    new EventHubsOrchestrationServiceSettings
                    {
                        MaxConcurrentTaskOrchestrationWorkItems = -1,
                    }));
        }
    }
}
