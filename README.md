# functions-scale-test


	• Create a storage account
		○ Create 5 queues, name them queue1, queue2, queue5
		○ Create a table called datapoints
		○ Save the connection string
		○ Download the csv file: https://github.com/dbarkol/functions-scale-test/blob/master/datapoints.typed.zip 
		○ Upload the csv file with the coordinates into the datapoints table

	• Create a CosmosDB instance (Core SQL)
		○ Create a database called weatherCollection
		○ Create a collection called weatherCollection
			§ Set the throughput to 40,000 Rus
			§ Set the partition key to /id
		○ Save the connection string

	• Create a Function App that will generate the storage queue messages
		○ Update the application settings with:
			§ StorageConnectionString	<your-storageaccount-connection-string>
			

	• Create 5 Function Apps that will process messages from each of the storage queues
		○ Update the application settings with:
			• CosmosDBConnectionString	<your-cosmos-connection-string>
			QueueConnectionString	<storage-account-connection-string>
			QueueName	The queue that you want to read from (queue1, queue2..)
			WeatherApiKey	<your weather api key>
				
