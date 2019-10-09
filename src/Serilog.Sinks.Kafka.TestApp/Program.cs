using Microsoft.Extensions.Configuration;
using System.IO;

namespace Serilog.Sinks.Kafka.TestApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .AddEnvironmentVariables()
                .Build();

            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(config)
                .CreateLogger();

            Log.Information("Console Application Test!");

            Log.CloseAndFlush();
        }
    }
}
