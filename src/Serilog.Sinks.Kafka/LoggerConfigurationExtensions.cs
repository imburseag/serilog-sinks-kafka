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
        /// <param name="errorHandler">kafka errorHandler</param>
        /// <param name="topic">The topic name.</param>
        /// <returns></returns>
        public static LoggerConfiguration KafkaBatch(
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
            Action<IProducer<Null, byte[]>, Error> errorHandler = null,
            ITextFormatter formatter = null)
        {
            return loggerConfiguration.KafkaBatch(
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
                errorHandler,
                formatter);
        }

        /// <summary>
        /// Adds a sink that writes log events to a Kafka topic in the broker endpoints.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="batchSizeLimit">The maximum number of events to include in a single batch.</param>
        /// <param name="period">The time in seconds to wait between checking for event batches.</param>
        /// <param name="bootstrapServers">The list of bootstrapServers separated by comma.</param>
        /// <param name="errorHandler">kafka errorHandler</param>
        /// <param name="topic">The topic name.</param>
        /// <returns></returns>
        public static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
            Func<LogEvent, string> topicDecider,
            string bootstrapServers = "localhost:9092",
            SecurityProtocol securityProtocol = SecurityProtocol.Plaintext,
            SaslMechanism? saslMechanism = null,
            string saslUsername = null,
            string saslPassword = null,
            string sslCaLocation = null,
            Action<IProducer<Null, byte[]>, Error> errorHandler = null,
            ITextFormatter formatter = null)
        {
            return loggerConfiguration.Kafka(
                bootstrapServers,
                securityProtocol,
                saslMechanism,
                saslUsername,
                saslPassword,
                sslCaLocation,
                topic: null,
                topicDecider,
                errorHandler,
                formatter);
        }

        public static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
            string topic,
            Func<LogEvent, string> topicDecider,
            ProducerConfig producerConfig,
            Action<IProducer<Null, byte[]>, Error> errorHandler = null,
            ITextFormatter formatter = null)
        {
            return loggerConfiguration.Kafka(producerConfig, topic, topicDecider, errorHandler, formatter);
        }

        public static LoggerConfiguration KafkaBatch(
            this LoggerSinkConfiguration loggerConfiguration,
            string topic,
            Func<LogEvent, string> topicDecider,
            ProducerConfig producerConfig,
            int batchSizeLimit = 50,
            int period = 5,
            Action<IProducer<Null, byte[]>, Error> errorHandler = null,
            ITextFormatter formatter = null)
        {
            return loggerConfiguration.KafkaBatch(producerConfig, topic, batchSizeLimit, period, topicDecider, errorHandler, formatter);
        }

        private static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
            string bootstrapServers,
            SecurityProtocol securityProtocol,
            SaslMechanism? saslMechanism,
            string saslUsername,
            string saslPassword,
            string sslCaLocation,
            string topic,
            Func<LogEvent, string> topicDecider,
            Action<IProducer<Null, byte[]>, Error> errorHandler,
            ITextFormatter formatter)
        {
            return loggerConfiguration.Kafka(new ProducerConfig()
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = securityProtocol,
                SaslMechanism = saslMechanism,
                SaslUsername = saslUsername,
                SaslPassword = saslPassword,
                SslCaLocation = sslCaLocation
            }, topic, topicDecider, errorHandler, formatter);
        }

        private static LoggerConfiguration KafkaBatch(
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
            Action<IProducer<Null, byte[]>, Error> errorHandler,
            ITextFormatter formatter)
        {
            return loggerConfiguration.KafkaBatch(new ProducerConfig()
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = securityProtocol,
                SaslMechanism = saslMechanism,
                SaslUsername = saslUsername,
                SaslPassword = saslPassword,
                SslCaLocation = sslCaLocation
            }, topic, batchSizeLimit, period, topicDecider, errorHandler, formatter);
        }

        private static LoggerConfiguration Kafka(
           this LoggerSinkConfiguration loggerConfiguration,
           ProducerConfig producerConfig,
           string topic,
           Func<LogEvent, string> topicDecider,
           Action<IProducer<Null, byte[]>, Error> errorHandler,
           ITextFormatter formatter)
        {
            var kafkaSink = new KafkaSink(
                producerConfig,
                topic,
                topicDecider,
                formatter, errorHandler);

            return loggerConfiguration
                .Sink(kafkaSink);
        }

        private static LoggerConfiguration KafkaBatch(
           this LoggerSinkConfiguration loggerConfiguration,
           ProducerConfig producerConfig,
           string topic,
           int batchSizeLimit,
           int period,
           Func<LogEvent, string> topicDecider,
           Action<IProducer<Null, byte[]>, Error> errorHandler,
           ITextFormatter formatter)
        {
            var kafkaSink = new KafkaBatchedSink(
                producerConfig,
                topic,
                topicDecider,
                formatter, errorHandler);

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
