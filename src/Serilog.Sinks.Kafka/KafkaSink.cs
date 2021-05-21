using Confluent.Kafka;
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
        private TopicPartition topic;
        private IProducer<Null, byte[]> producer;
        private ITextFormatter formatter;
        private readonly Func<LogEvent, string> _topicDecider;

        public KafkaSink(
            string bootstrapServers,
            int batchSizeLimit,
            int period,
            SecurityProtocol securityProtocol,
            SaslMechanism saslMechanism,
            string topic,
            string saslUsername,
            string saslPassword,
            string sslCaLocation,
            ITextFormatter formatter = null) : base(batchSizeLimit, TimeSpan.FromSeconds(period))
        {
            ConfigureKafkaConnection(bootstrapServers, securityProtocol, saslMechanism, saslUsername,
                saslPassword, sslCaLocation);

            this.formatter = formatter ?? new Formatting.Json.JsonFormatter(renderMessage: true);

            this.topic = new TopicPartition(topic, Partition.Any);
        }

        public KafkaSink(
            string bootstrapServers,
            int batchSizeLimit,
            int period,
            SecurityProtocol securityProtocol,
            SaslMechanism saslMechanism,
            Func<LogEvent, string> topicDecider,
            string saslUsername,
            string saslPassword,
            string sslCaLocation,
            ITextFormatter formatter = null) : base(batchSizeLimit, TimeSpan.FromSeconds(period))
        {
            ConfigureKafkaConnection(bootstrapServers, securityProtocol, saslMechanism, saslUsername,
                saslPassword, sslCaLocation);

            this.formatter = formatter ?? new Formatting.Json.JsonFormatter(renderMessage: true);

            this._topicDecider = topicDecider;
        }

        protected override async Task EmitBatchAsync(IEnumerable<LogEvent> events)
        {
            var tasks = new List<Task>();

            foreach (var logEvent in events)
            {
                using (var render = new StringWriter(CultureInfo.InvariantCulture))
                {
                    formatter.Format(logEvent, render);
                    var message = new Message<Null, byte[]> { Value = Encoding.UTF8.GetBytes(render.ToString()) };

                    var kakfaTopicPartition = _topicDecider != null
                        ? new TopicPartition(_topicDecider(logEvent), Partition.Any)
                        : topic;

                    tasks.Add(producer.ProduceAsync(kakfaTopicPartition, message));
                }
            }

            await Task.WhenAll(tasks);
        }

        private void ConfigureKafkaConnection(string bootstrapServers, SecurityProtocol securityProtocol,
            SaslMechanism saslMechanism, string saslUsername, string saslPassword, string sslCaLocation)
        {
            var config = new ProducerConfig()
                .SetValue("ApiVersionFallbackMs", 0)
                .SetValue("EnableDeliveryReports", false)
                .LoadFromEnvironmentVariables()
                .SetValue("BootstrapServers", bootstrapServers)
                .SetValue("SecurityProtocol", securityProtocol)
                .SetValue("SaslMechanism", saslMechanism)
                .SetValue("SslCaLocation",
                    string.IsNullOrEmpty(sslCaLocation)
                        ? null
                        : Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), sslCaLocation))
                .SetValue("SaslUsername", saslUsername)
                .SetValue("SaslPassword", saslPassword);

            producer = new ProducerBuilder<Null, byte[]>(config)
                .Build();
        }
    }
}