using Confluent.Kafka;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using System;

namespace Serilog.Sinks.Kafka
{
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
            string sslCaLocation = null,
            ITextFormatter formatter = null,
            LogEventLevel restrictedToMinLevel = LogEventLevel.Verbose,
            LoggingLevelSwitch levelSwitch = null)
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
                sslCaLocation,
                formatter);

            return loggerConfiguration.Sink(sink, restrictedToMinLevel, levelSwitch);
        }

        public static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
            Func<LogEvent, string> topicDecider,
            string bootstrapServers = "localhost:9092",
            int batchSizeLimit = 50,
            int period = 5,
            SecurityProtocol securityProtocol = SecurityProtocol.Plaintext,
            SaslMechanism saslMechanism = SaslMechanism.Plain,
            string saslUsername = null,
            string saslPassword = null,
            string sslCaLocation = null,
            ITextFormatter formatter = null,
            LogEventLevel restrictedToMinLevel = LogEventLevel.Verbose,
            LoggingLevelSwitch levelSwitch = null)
        {
            var sink = new KafkaSink(
                bootstrapServers,
                batchSizeLimit,
                period,
                securityProtocol,
                saslMechanism,
                topicDecider,
                saslUsername,
                saslPassword,
                sslCaLocation,
                formatter);

            return loggerConfiguration.Sink(sink, restrictedToMinLevel, levelSwitch);
        }

        public static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
           KafkaSinkOptions options, LogEventLevel restrictedToMinLevel = LogEventLevel.Verbose,
            LoggingLevelSwitch levelSwitch = null)
        {
            var sink = new KafkaSink(options);

            return loggerConfiguration.Sink(sink, restrictedToMinLevel, levelSwitch);
        }
    }
}