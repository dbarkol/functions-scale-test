using System;
using System.Collections.Generic;
using System.Text;

namespace ForecastGenerator
{
    /// <summary>
    /// This class supports the data we want to pass along to the activity function.
    /// </summary>
    public class ForecastMessageDetails
    {
        public string PartitionKey { get; set; }

        public string QueueName { get; set; }

    }
}
