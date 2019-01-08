using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Azure.ServiceBus;
using Models;
using Newtonsoft.Json;

namespace ForecastGenerator
{
    public static class SendCoordinates
    {
        #region Data Members

        // Connection strings for service bus and storage queues
        private static readonly string StorageConnectionString = Environment.GetEnvironmentVariable("StorageConnectionString");
        private static readonly string ServiceBusConnectionString = Environment.GetEnvironmentVariable("ServiceBusConnectionString");

        #endregion

        #region Orchestrators

        [FunctionName("SendCoordinatesOrchestrator")]
        public static async Task Run([OrchestrationTrigger] DurableOrchestrationContext ctx)
        {
            // The number of queues that will be filled with
            // forcasts requests.
            const int numberOfQueues = 5;

            // Kick off a task for each queue
            var parallelTasks = new List<Task>();
            for (var n = 0; n < numberOfQueues; n++) 
            {
                var t = ctx.CallSubOrchestratorAsync("QueueOrchestrator", n+1);
                parallelTasks.Add(t);
            }

            await Task.WhenAll(parallelTasks);
        }

        [FunctionName("QueueOrchestrator")]
        public static async Task QueueOrchestrator(
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
                var queueName = $"queue{queueNumber}";          

                var details =
                    new ForecastMessageDetails
                    {
                        PartitionKey = partitionKey,
                        QueueName = queueName
                    };

                var t = ctx.CallActivityAsync("GenerateForecastStorageQueueActivity", JsonConvert.SerializeObject(details));
                parallelTasks.Add(t);
            }

            await Task.WhenAll(parallelTasks);
        }

        #endregion

        #region Activity Functions

        /// <summary>
        /// This activity function retrieves a list of coordinates from a table and 
        /// creates a message for each of them in a storage queue.
        /// </summary>
        /// <param name="details">Details about the partition key and queue to work with</param>
        /// <returns></returns>
        [FunctionName("GenerateForecastStorageQueueActivity")]
        public static async Task GenerateForecastStorageQueueActivity([ActivityTrigger] string details)
        {
            // Deserialize the forecast details so that we can determine which partition
            // key and queue to work with. 
            var messageDetails = JsonConvert.DeserializeObject<ForecastMessageDetails>(details);

            // Get a reference to the table that will contain all the coordinates.
            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference("datapoints");
            await table.CreateIfNotExistsAsync();

            // Get a reference to the storage queue that we will send messages to.
            var queueClient = storageAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(messageDetails.QueueName);
            await queue.CreateIfNotExistsAsync();
            
            // Get the coordinates from the table and add each one as a separate 
            // message to the queue. 
            var coordinates = await GetCoordinatesFromStorage(messageDetails.PartitionKey, table);
            foreach (var c in coordinates)
            {
                var forecastRequest = new List<Coordinates> { new Coordinates { Latitude = c.Latitude, Longitude = c.Longitude } };
                var serializedMessage = JsonConvert.SerializeObject(forecastRequest);
                var cloudQueueMessage = new CloudQueueMessage(serializedMessage);
                await queue.AddMessageAsync(cloudQueueMessage);
            }
        }

        /// <summary>
        /// This activity functon retrieves a list of coordinates from a table and
        /// creates a message for each of them in a service bus queue.
        /// </summary>
        /// <param name="details">Details about the partition key and queue to work with</param>
        /// <returns></returns>
        [FunctionName("GenerateForecastServiceBusActivity")]
        public static async Task GenerateForecastServiceBusActivity([ActivityTrigger] string details)
        {
            // Deserialize the forecast details so that we can determine which partion
            // key and queue to work with. 
            var messageDetails = JsonConvert.DeserializeObject<ForecastMessageDetails>(details);

            // Get a reference to the table that will contain all the coordinates.
            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference("datapoints");
            await table.CreateIfNotExistsAsync();

            // Get a reference to the service bus queue that we will send the messages to.
            var queueClient = new Microsoft.Azure.ServiceBus.QueueClient(ServiceBusConnectionString, messageDetails.QueueName);

            // Get the coordinates from the table and add each one as a separate message
            // to the service bus queue.
            var coordinates = await GetCoordinatesFromStorage(messageDetails.PartitionKey, table);           
            var messagesToSend = new List<Message>();
            foreach (var c in coordinates)
            {
                var forecastRequest = new List<Coordinates> { new Coordinates { Latitude = c.Latitude, Longitude = c.Longitude } };
                var serializedMessage = JsonConvert.SerializeObject(forecastRequest);
                var message = new Message(Encoding.UTF8.GetBytes(serializedMessage));
                messagesToSend.Add(message);
            }

            // Service bus allows you to add messages in a batch.
            await queueClient.SendAsync(messagesToSend);
        }

        #endregion

        #region Private Methods

        private static async Task<List<CoordinateEntity>> GetCoordinatesFromStorage(string partitionKey, CloudTable table)
        {
            // Assumption: Each partition will contain a maximum of 1000 records.

            // Format the query for retrieving records by a
            // partition key. The assumption here is that there
            // are no more than 1000 records for each partition.
            var query = new TableQuery<CoordinateEntity>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            // Retreive the records and add them to the collection
            var allEntities = new List<CoordinateEntity>();
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
