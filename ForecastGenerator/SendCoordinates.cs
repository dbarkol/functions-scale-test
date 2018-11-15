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
using Microsoft.WindowsAzure.Storage.Table;
using Models;
using Newtonsoft.Json;

namespace ForecastGenerator
{
    public static class SendCoordinates
    {
        #region Data Members

        private static readonly string StorageConnectionString = Environment.GetEnvironmentVariable("StorageConnectionString");

        #endregion

        #region Orchestrators

        [FunctionName("SendCoordinates_Orchestrator")]
        public static async Task Run([OrchestrationTrigger] DurableOrchestrationContext ctx)
        {
            // List of queues that we want to fill
            string[] queueNumbers = {"1", "2", "3", "4", "5"};

            // Kick of a task for each queue
            var parallelTasks = new List<Task>();
            foreach (var n in queueNumbers)
            {
                var t = ctx.CallSubOrchestratorAsync("Queue_Orchestrator", n);
                parallelTasks.Add(t);
            }

            await Task.WhenAll(parallelTasks);
        }

        [FunctionName("Queue_Orchestrator")]
        public static async Task SendMessagesToQueue(
            [OrchestrationTrigger] DurableOrchestrationContext ctx)
        {
            // Kick off the tasks to send messages to the queue
            var queueNumber = ctx.GetInput<string>();
            var parallelTasks = new List<Task>();
            for (var i = 0; i < 100; i++)
            {                
                // Pass the details about which queue and partition to use 
                // when sending the messages.
                var partitionKey = $"{queueNumber}-{i}";
                var details =
                    new ForecastMessageDetails
                    {
                        PartitionKey = partitionKey,
                        QueueName = $"queue{queueNumber}"
                    };
                Task t = ctx.CallActivityAsync("GenerateForecastActivity", JsonConvert.SerializeObject(details));
                parallelTasks.Add(t);
            }

            await Task.WhenAll(parallelTasks);
        }

        #endregion

        #region Activities

        [FunctionName("GenerateForecastActivity")]
        public static async Task GenerateForecast([ActivityTrigger] string details)
        {
            var messageDetails = JsonConvert.DeserializeObject<ForecastMessageDetails>(details);

            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference("datapoints");
            var queueClient = storageAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(messageDetails.QueueName);
            await table.CreateIfNotExistsAsync();

            List<CoordinateEntity> coordinates = await GetCoordinatesFromStorage(messageDetails.PartitionKey, table);
            foreach (var c in coordinates)
            {
                var forecastRequest = new List<Coordinates> { new Coordinates { Latitude = c.Latitude, Longitude = c.Longitude } };
                var serializedMessage = JsonConvert.SerializeObject(forecastRequest);
                var cloudQueueMessage = new CloudQueueMessage(serializedMessage);
                await queue.AddMessageAsync(cloudQueueMessage);
            }
        }

        #endregion

        #region Private Methods

        private static async Task<List<CoordinateEntity>> GetCoordinatesFromStorage(string partitionKey, CloudTable table)
        {
            var query = new TableQuery<CoordinateEntity>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            List<CoordinateEntity> allEntities = new List<CoordinateEntity>();

            TableContinuationToken tableContinuationToken = null;
            do
            {
                var queryResponse = await table.ExecuteQuerySegmentedAsync<CoordinateEntity>(query, tableContinuationToken, null, null);
                tableContinuationToken = queryResponse.ContinuationToken;
                allEntities.AddRange(queryResponse.Results);
            }
            while (tableContinuationToken != null);

            return allEntities;
        }

        #endregion

    }
}
