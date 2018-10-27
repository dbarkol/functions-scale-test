using System;
using System.Threading.Tasks;
using System.Net.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Models;

namespace ForecastProcessor
{
    public static class GetForecasts
    {
        #region Private Data Members

        // It is important that we instatiate this outside the scope of the function so
        // that it can be reused with each invocation. 
        private static readonly HttpClient Client = new HttpClient();

        // Weather.com API key
        private static readonly string ApiKey = System.Environment.GetEnvironmentVariable("WeatherApiKey");

        #endregion

        [FunctionName("GetForecasts")]
        public static async Task Run(
            [QueueTrigger("coordinates", Connection = "QueueConnectionString")]Coordinates[] items,         // collection of coordinates in the request
            [CosmosDB(
                databaseName: "weatherDatabase",
                collectionName: "weatherCollection",
                ConnectionStringSetting = "CosmosDBConnectionString")] IAsyncCollector<Forecast> documents, // output binding to CosmosDB
            ILogger log)
        {
            // Iterate through the collection of coordinates, retrieve
            // the forecast and then store it in a new document.           
            foreach (var coordinates in items)
            {
                // Format the API request with the coordinates and API key
                var apiRequest =
                    $"https://api.weather.com/v1/geocode/{coordinates.Latitude}/{coordinates.Longitude}/forecast/fifteenminute.json?language=en-US&units=e&apiKey={ApiKey}";

                // Make the forecast request and read the response
                var response = await Client.GetAsync(apiRequest);
                var forecast = await response.Content.ReadAsStringAsync();

                // Add a new document with the forecast details
                await documents.AddAsync(new Forecast
                {
                    Latitude = coordinates.Latitude,
                    Longitude = coordinates.Longitude,
                    Result = forecast
                });
            }
        }
    }
}
