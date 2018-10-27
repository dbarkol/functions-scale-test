using Newtonsoft.Json;

namespace Models
{
    public class Forecast
    {
        #region Properties

        [JsonProperty("latitude")] public double Latitude { get; set; }

        [JsonProperty("longitude")] public double Longitude { get; set; }

        [JsonProperty("result")] public string Result { get; set; }

        #endregion
    }
}
