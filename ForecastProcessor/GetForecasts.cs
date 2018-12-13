using System;
using System.Threading.Tasks;
using System.Net.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Models;
using Newtonsoft.Json;

namespace ForecastProcessor
{
    public static class GetForecasts
    {
        #region Private Data Members

        // It is important that we instantiate this outside the scope of the function so
        // that it can be reused with each invocation. 
        private static readonly HttpClient Client = new HttpClient();

        // Weather.com API key
        private static readonly string ApiKey = System.Environment.GetEnvironmentVariable("WeatherApiKey");

        #endregion


        [FunctionName("GetForecasts")]
        public static async Task Run(
            [QueueTrigger("%QueueName%", Connection = "QueueConnectionString")]Coordinates[] items,
            [EventHub("%EventHubName%", Connection = "EventHubsConnectionString")] IAsyncCollector<Forecast> results,
            ILogger log)
        {
            log.LogInformation("GetForecasts triggered");

            // Iterate through the collection of coordinates, retrieve
            // the forecast and then pass it along.    
            foreach (var c in items)
            { 
                // Format the API request with the coordinates and API key
                var apiRequest =
                    $"https://api.weather.com/v1/geocode/{c.Latitude}/{c.Longitude}/forecast/fifteenminute.json?language=en-US&units=e&apiKey={ApiKey}";

                // Make the forecast request and read the response
                var response = await Client.GetAsync(apiRequest);
                var forecast = await response.Content.ReadAsStringAsync();
                log.LogInformation(forecast);

                // Important note:
                // Uncomment this code if you have configured Event Hubs with
                // enough throughput units to handle the ingress of messages. If
                // this is being scaled out to roughly 5 functions apps (not instances)
                // running in parallel, the default maximum limit of units (20) will
                // not enough and request will be throttled.

                // Send to an event hub
                //await results.AddAsync(new Forecast
                //{
                //    Longitude = c.Longitude,
                //    Latitude = c.Latitude,
                //    Result = forecast
                //});
            }
            
        }
    }
}
