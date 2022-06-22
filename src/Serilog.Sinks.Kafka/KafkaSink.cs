using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.Kafka
{
    public class KafkaSink : IBatchedLogEventSink
    {
        private const int FlushTimeoutSecs = 10;

        private readonly TopicPartition _globalTopicPartition;
        private readonly ITextFormatter _formatter;
        private readonly Func<LogEvent, string> _topicDecider;
        private IProducer<Null, byte[]> _producer;
        Action<IProducer<Null, byte[]>, Error> _errorHandler;

        public KafkaSink(
            string bootstrapServers,
            SecurityProtocol securityProtocol,
            SaslMechanism? saslMechanism,
            string saslUsername,
            string saslPassword,
            string sslCaLocation,
            string topic = null,
            Func<LogEvent, string> topicDecider = null,
            ITextFormatter formatter = null,
            Action<IProducer<Null, byte[]>, Error> errorHandler = null)
        {
            ConfigureKafkaConnection(
                bootstrapServers,
                securityProtocol,
                saslMechanism,
                saslUsername,
                saslPassword,
                sslCaLocation);

            _formatter = formatter ?? new Formatting.Json.JsonFormatter(renderMessage: true);

            if (topic != null)
                _globalTopicPartition = new TopicPartition(topic, Partition.Any);

            if (topicDecider != null)
                _topicDecider = topicDecider;

            if (_errorHandler != null)
                _errorHandler = errorHandler;
            else
            {
                _errorHandler = (pro, msg) =>
                {
                    Log.Error($"{msg.Reason}");
                };
            }
        }

        public Task OnEmptyBatchAsync() => Task.CompletedTask;

        public Task EmitBatchAsync(IEnumerable<LogEvent> batch)
        {
            foreach (var logEvent in batch)
            {
                Message<Null, byte[]> message;

                var topicPartition = _topicDecider == null
                    ? _globalTopicPartition
                    : new TopicPartition(_topicDecider(logEvent), Partition.Any);

                using (var render = new StringWriter(CultureInfo.InvariantCulture))
                {
                    _formatter.Format(logEvent, render);

                    message = new Message<Null, byte[]>
                    {
                        Value = Encoding.UTF8.GetBytes(render.ToString())
                    };
                }

                _producer.Produce(topicPartition, message);
            }

            _producer.Flush(TimeSpan.FromSeconds(FlushTimeoutSecs));

            return Task.CompletedTask;
        }

        private void ConfigureKafkaConnection(
            string bootstrapServers,
            SecurityProtocol securityProtocol,
            SaslMechanism? saslMechanism,
            string saslUsername,
            string saslPassword,
            string sslCaLocation)
        {
            var config = new ProducerConfig()
                .SetValue("ApiVersionFallbackMs", 0)
                .SetValue("EnableDeliveryReports", false)
                .LoadFromEnvironmentVariables()
                .SetValue("BootstrapServers", bootstrapServers)
                .SetValue("SecurityProtocol", securityProtocol)
                .SetValue("SslCaLocation",
                    string.IsNullOrEmpty(sslCaLocation)
                        ? null
                        : Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), sslCaLocation))
                .SetValue("SaslUsername", saslUsername)
                .SetValue("SaslPassword", saslPassword);

            if (saslMechanism.HasValue)
            {
                config.SetValue("SaslMechanism", saslMechanism);
            }

            _producer = new ProducerBuilder<Null, byte[]>(config)
                .SetErrorHandler(_errorHandler)
                .Build();
        }
    }
}