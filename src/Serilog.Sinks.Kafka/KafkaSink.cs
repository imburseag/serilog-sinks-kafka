using Confluent.Kafka;
using Serilog.Configuration;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Sinks.PeriodicBatching;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Serilog.Sinks.Kafka
{
    public class KafkaSink : PeriodicBatchingSink
    {
        private string topic;
        private IProducer<Null, byte[]> producer;
        private ITextFormatter formatter;

        public KafkaSink(
            string bootstrapServers,
            int batchSizeLimit,
            int period,
            SecurityProtocol securityProtocol,
            SaslMechanism saslMechanism,
            string topic,
            string saslUsername,
            string saslPassword,
            string sslCaLocation) : base(batchSizeLimit, TimeSpan.FromSeconds(period))
        {
            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = securityProtocol,
                SaslMechanism = saslMechanism,
                SslCaLocation = string.IsNullOrEmpty(sslCaLocation) ? null : Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), sslCaLocation),
                SaslUsername = saslUsername,
                SaslPassword = saslPassword,
                ApiVersionFallbackMs = 0,
                //Debug = "security,broker,protocol"
            };

            producer = new ProducerBuilder<Null, byte[]>(config)
                .Build();

            formatter = new Formatting.Json.JsonFormatter(renderMessage: true);
            this.topic = topic;
        }

        protected override async Task EmitBatchAsync(IEnumerable<LogEvent> events)
        {
            foreach (var logEvent in events)
            {
                using (var render = new StringWriter(CultureInfo.InvariantCulture))
                {
                    formatter.Format(logEvent, render);
                    var message = new Message<Null, byte[]> { Value = Encoding.UTF8.GetBytes(render.ToString()) };
                    await producer.ProduceAsync(topic, message);
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            producer?.Dispose();
            base.Dispose(disposing);
        }
    }

    public static class LoggerConfigurationKafkaExtensions
    {
        /// <summary>
        /// Adds a sink that writes log events to a Kafka topic in the broker endpoints.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="batchSizeLimit">The maximum number of events to include in a single batch.</param>
        /// <param name="period">The time in seconds to wait between checking for event batches.</param>
        /// <param name="bootstrapServers">The list of bootstrapServers separated by comma.</param>
        /// <param name="topic">The topic name.</param>
        /// <returns></returns>
        public static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
            string bootstrapServers = "localhost:9092",
            int batchSizeLimit = 50,
            int period = 5,
            SecurityProtocol securityProtocol = SecurityProtocol.Plaintext,
            SaslMechanism saslMechanism = SaslMechanism.Plain,
            string topic = "logs",
            string saslUsername = null,
            string saslPassword = null,
            string sslCaLocation = null)
        {
            var sink = new KafkaSink(
                bootstrapServers,
                batchSizeLimit,
                period,
                securityProtocol,
                saslMechanism,
                topic,
                saslUsername,
                saslPassword,
                sslCaLocation);

            return loggerConfiguration.Sink(sink);
        }
    }
}