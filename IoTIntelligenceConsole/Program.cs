using System;
using StackExchange.Redis;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Text;

namespace IoTIntelligenceConsole
{
    class Program
    {
        static async Task Main(string[] args)
        {
            IDatabase database = lazyConnection.Value.GetDatabase();
            String cacheKey = "IoTInt.Machine1226.PartOut";

            try
            {
                double minTemperature = 20;
                double minHumidity = 60;

                Random rand = new Random();

                while (true)
                {
                    double currentTemperature = minTemperature + rand.NextDouble() * 15;
                    double currentHumidity = minHumidity + rand.NextDouble() * 20;
                    int output = 0;

                    if (rand.NextDouble() > .8) output = 1;

                    // Create JSON message  

                    var telemetryDataPoint = new
                    {
                        deviceId = cacheKey,
                        temperature = currentTemperature,
                        humidity = currentHumidity,
                        output = output,
                        value = currentTemperature
                    };

                    string messageString = "";

                    messageString = JsonConvert.SerializeObject(telemetryDataPoint);

                    //var message = new Message(Encoding.ASCII.GetBytes(messageString));

                    //Console.WriteLine(database.StringGet("IoTStream"));

                    database.StringSet("IoTStream", messageString);

                    var redisData = new { uts = DateTimeOffset.UtcNow, val = output };

                    await database.SortedSetAddAsync(cacheKey, JsonConvert.SerializeObject(redisData), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

                    Console.WriteLine(database.StringGet("IoTStream"));

                    await Task.Delay(1000 * 10);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            string connectionString = "mds365demo1.redis.cache.windows.net:6380,password=Ty9xAgNXtMb6gZY9lpVViSWGRpxvi9Z2xUvyODtOSW8=,ssl=True,abortConnect=False";

            return ConnectionMultiplexer.Connect(connectionString);
        });

        public static ConnectionMultiplexer Connection 
        { 
            get 
            {
                return lazyConnection.Value;
            } 
        }
    }
}
