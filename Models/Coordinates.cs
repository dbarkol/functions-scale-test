using Newtonsoft.Json;

namespace Models
{
    public class Coordinates
    {
        #region Properties

        [JsonProperty("latitude")]
        public double Latitude { get; set; }

        [JsonProperty("longitude")]
        public double Longitude { get; set; }

        #endregion

    }
}
