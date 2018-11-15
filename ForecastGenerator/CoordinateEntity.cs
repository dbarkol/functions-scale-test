using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;

namespace ForecastGenerator
{
    public class CoordinateEntity : TableEntity
    {
        public CoordinateEntity()
        {
        }

        public CoordinateEntity(double longitude, double latitude, string partitionKey)
        {
            this.Longitude = longitude;
            this.Latitude = latitude;
            this.PartitionKey = partitionKey;
            this.RowKey = Guid.NewGuid().ToString();
        }

        public double Longitude { get; set; }

        public double Latitude { get; set; }
    }
}
