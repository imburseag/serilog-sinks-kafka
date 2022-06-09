using System;
using Confluent.Kafka;
using Serilog.Configuration;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.Kafka
{
    public static class LoggerConfigurationExtensions
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
            SaslMechanism? saslMechanism = null,
            string topic = "logs",
            string saslUsername = null,
            string saslPassword = null,
            string sslCaLocation = null,
            ITextFormatter formatter = null)
        {
            return loggerConfiguration.Kafka(
                bootstrapServers,
                batchSizeLimit,
                period,
                securityProtocol,
                saslMechanism,
                saslUsername,
                saslPassword,
                sslCaLocation,
                topic,
                topicDecider: null,
                formatter);
        }

        public static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
            Func<LogEvent, string> topicDecider,
            string bootstrapServers = "localhost:9092",
            int batchSizeLimit = 50,
            int period = 5,
            SecurityProtocol securityProtocol = SecurityProtocol.Plaintext,
            SaslMechanism? saslMechanism = null,
            string saslUsername = null,
            string saslPassword = null,
            string sslCaLocation = null,
            ITextFormatter formatter = null)
        {
            return loggerConfiguration.Kafka(
                bootstrapServers,
                batchSizeLimit,
                period,
                securityProtocol,
                saslMechanism,
                saslUsername,
                saslPassword,
                sslCaLocation,
                topic: null,
                topicDecider,
                formatter);
        }

        private static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
            string bootstrapServers,
            int batchSizeLimit,
            int period,
            SecurityProtocol securityProtocol,
            SaslMechanism? saslMechanism,
            string saslUsername,
            string saslPassword,
            string sslCaLocation,
            string topic,
            Func<LogEvent, string> topicDecider,
            ITextFormatter formatter)
        {
            var kafkaSink = new KafkaSink(
                bootstrapServers,
                securityProtocol,
                saslMechanism,
                saslUsername,
                saslPassword,
                sslCaLocation,
                topic,
                topicDecider,
                formatter);

            var batchingOptions = new PeriodicBatchingSinkOptions
            {
                BatchSizeLimit = batchSizeLimit,
                Period = TimeSpan.FromSeconds(period)
            };

            var batchingSink = new PeriodicBatchingSink(
                kafkaSink,
                batchingOptions);

            return loggerConfiguration
                .Sink(batchingSink);
        }
    }
}
