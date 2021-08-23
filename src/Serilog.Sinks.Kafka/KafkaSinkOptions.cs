using Confluent.Kafka;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using System;

namespace Serilog.Sinks.Kafka
{
    public class KafkaSinkOptions
    {
        public string BootstrapServers { get; set; } = "localhost:9092";
        public int BatchSizeLimit { get; set; } = 50;
        public int Period { get; set; } = 5;
        public SecurityProtocol SecurityProtocol { get; set; } = SecurityProtocol.Plaintext;
        public SaslMechanism SaslMechanism { get; set; } = SaslMechanism.Plain;
        public string Topic { get; set; } = "logs";
        public string SaslUsername { get; set; } = null;
        public string SaslPassword { get; set; } = null;
        public string SslCaLocation { get; set; } = null;
        public ITextFormatter Formatter { get; set; } = null;
        public Func<LogEvent, string> TopicDecider { get; set; } = null;

        /// <summary>
        /// Specifies how failing emits should be handled.
        /// </summary>
        public EmitEventFailureHandling EmitEventFailure { get; set; }

        /// <summary>
        /// Sink to use when kafka is unable to accept the events. This is optional and depends on the EmitEventFailure setting.
        /// </summary>
        public ILogEventSink FailureSink { get; set; }

        /// <summary>
        /// A callback which can be used to handle logevents which are not submitted to kafka
        /// like when it is unable to accept the events. This is optional and depends on the EmitEventFailure setting.
        /// </summary>
        public Action<LogEvent> FailureCallback { get; set; }

    }
}