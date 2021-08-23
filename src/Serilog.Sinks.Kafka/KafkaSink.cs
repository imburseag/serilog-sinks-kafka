using Confluent.Kafka;
using Serilog.Debugging;
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
        private readonly Func<LogEvent, string> topicDecider;
        private KafkaSinkOptions _options;

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

            this.topicDecider = topicDecider;
        }

        public KafkaSink(KafkaSinkOptions options)
            : base(options.BatchSizeLimit, TimeSpan.FromSeconds(options.Period))
        {
            _options = options;

            ConfigureKafkaConnection(options.BootstrapServers,
                options.SecurityProtocol,
                options.SaslMechanism,
                options.SaslUsername,
                 options.SaslPassword,
                options.SslCaLocation);

            formatter = options.Formatter ?? new Formatting.Json.JsonFormatter(renderMessage: true);
            topicDecider = options.TopicDecider;
            topic = new TopicPartition(options.Topic, Partition.Any);
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

                    var kafkaTopicPartition = topicDecider != null
                        ? new TopicPartition(topicDecider(logEvent), Partition.Any)
                        : topic;

                    tasks.Add(producer.ProduceAsync(kafkaTopicPartition, message));
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

        /// <summary>
        /// Handles the exceptions.
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="events"></param>
        protected virtual void HandleException(Exception ex, IEnumerable<LogEvent> events)
        {
            if (_options.EmitEventFailure.HasFlag(EmitEventFailureHandling.WriteToSelfLog))
            {
                SelfLog.WriteLine("Caught exception while preforming bulk operation to Kafka: {0}", ex);
            }
            if (_options.EmitEventFailure.HasFlag(EmitEventFailureHandling.WriteToFailureSink) &&
                _options.FailureSink != null)
            {
                try
                {
                    foreach (var e in events)
                    {
                        _options.FailureSink.Emit(e);
                    }
                }
                catch (Exception exSink)
                {
                    // We do not let this fail too
                    SelfLog.WriteLine("Caught exception while emitting to sink {1}: {0}", exSink,
                        _options.FailureSink);
                }
            }
            if (_options.EmitEventFailure.HasFlag(EmitEventFailureHandling.RaiseCallback) &&
                       _options.FailureCallback != null)
            {
                try
                {
                    foreach (var e in events)
                    {
                        _options.FailureCallback(e);
                    }
                }
                catch (Exception exCallback)
                {
                    SelfLog.WriteLine("Caught exception while emitting to callback {1}: {0}", exCallback,
                        _options.FailureCallback);
                }
            }
            if (_options.EmitEventFailure.HasFlag(EmitEventFailureHandling.ThrowException))
                throw ex;
        }
    }
}