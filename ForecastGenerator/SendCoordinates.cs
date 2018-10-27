using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Models;
using Newtonsoft.Json;

namespace ForecastGenerator
{
    public static class SendCoordinates
    {
        private static readonly string QueueConnectionString = Environment.GetEnvironmentVariable("QueueConnectionString");
        private static readonly string QueueName = Environment.GetEnvironmentVariable("QueueName");

        [FunctionName("SendCoordinates")]
        public static async Task Run([OrchestrationTrigger] DurableOrchestrationContext ctx)
        {
            var parallelTasks = new List<Task>();

            for (var i = 0; i < 1000; i++)
            {
                Task t = ctx.CallActivityAsync("SendCoordinatesActivity", 100);
                parallelTasks.Add(t);
            }

            await Task.WhenAll(parallelTasks);

        }

        [FunctionName("SendCoordinatesActivity")]
        public static async Task SendCoordinatesActivity([ActivityTrigger] int count)
        {
            var storageAccount = CloudStorageAccount.Parse(QueueConnectionString);

            // Create the queue client.
            var queueClient = storageAccount.CreateCloudQueueClient();

            // Retrieve a reference to a queue.
            var queue = queueClient.GetQueueReference(QueueName);

            for (var i = 0; i < count; i++)
            {
                var coordinates = new List<Coordinates> {new Coordinates {Latitude = 33.12, Longitude = -113.42}};
                var serializedMessage = JsonConvert.SerializeObject(coordinates);
                var cloudQueueMessage = new CloudQueueMessage(serializedMessage);
                await queue.AddMessageAsync(cloudQueueMessage);
            }
        }

    }
}
