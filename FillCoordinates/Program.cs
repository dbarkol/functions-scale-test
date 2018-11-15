using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading.Tasks;

namespace FillCoordinates
{
    /*
        This console application is just a brute-force tool for adding entities
        to a storage account table. 
    */
    class Program
    {
        #region Private Data Members

        private const string ConnectionString = "<storage-account-connection-string>";
        private static CloudStorageAccount _storageAccount;
        private static CloudTableClient _tableClient;
        private static CloudTable _table;

        #endregion

        static void Main(string[] args)
        {
            Console.WriteLine("Press <enter> to start");
            Console.ReadLine();

            Initialize();
            Fill();

            Console.WriteLine("Done");
            Console.ReadLine();
        }

        /// <summary>
        /// This is just a test method to see if we can read from a partition.
        /// </summary>
        /// <returns></returns>
        private static async Task<List<CoordinateEntity>> TestRead()
        {           
            var query = new TableQuery<CoordinateEntity>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "1-40"));

            List<CoordinateEntity> allEntities = new List<CoordinateEntity>(); 

            TableContinuationToken tableContinuationToken = null;
            do
            {
                var queryResponse = await _table.ExecuteQuerySegmentedAsync<CoordinateEntity>(query, tableContinuationToken, null, null);
                tableContinuationToken = queryResponse.ContinuationToken;
                allEntities.AddRange(queryResponse.Results);
            }
            while (tableContinuationToken != null);

            return allEntities;
        }

        private static void Fill()
        {
            // The number indicates the prefix for the partition key. This
            // will eventually coorelate to the queues (queue1, queue2, etc.)
            // that the coordinates are read from.
            InsertCoordinates("1");
            InsertCoordinates("2");
            InsertCoordinates("3");
            InsertCoordinates("4");
            InsertCoordinates("5");
        }

        private static void Initialize()
        {
            _storageAccount = CloudStorageAccount.Parse(ConnectionString);
            _tableClient = _storageAccount.CreateCloudTableClient();
            _table = _tableClient.GetTableReference("datapoints");
            _table.CreateIfNotExistsAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Table storage only allows you to insert 100 records at a time. 
        /// Also, you can only read 1000 records at a time. 
        /// </summary>
        /// <param name="partitionKey"></param>
        private static void InsertCoordinates(string partitionKey)
        {
            // This function will add 100 entities at a time. It will
            // also update the partition key for each 1000 records so that
            // they can be read at once.

            var totalCoordinates = 0;
            var partitionIndex = 0;
            for (var c = 0; c < 1000; c++)
            {                
                var queryPartitionKey = $"{partitionKey}-{partitionIndex.ToString()}";

                var batchOperation = new TableBatchOperation();
                for (var i = 0; i < 100; i++)
                {
                    var coordinate = new CoordinateEntity(
                        latitude: GetRandomNumber(33.10000, 33.120000),
                        longitude: GetRandomNumber(-113.00000, -113.410000),
                        partitionKey: queryPartitionKey);

                    // Insert 100 records
                    batchOperation.Insert(coordinate);
                }
                _table.ExecuteBatchAsync(batchOperation).GetAwaiter().GetResult();

                totalCoordinates += 100;
                partitionIndex = (totalCoordinates/1000);
                Console.WriteLine($"Total coordinates for partition {queryPartitionKey}: {totalCoordinates} ");
            }
        }

        private static double GetRandomNumber(double minimum, double maximum)
        {
            var random = new Random();
            return random.NextDouble() * (maximum - minimum) + minimum;
        }
    }
}
