using Confluent.Kafka;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using System;

namespace Serilog.Sinks.Kafka
{
    public class KafkaSinkOptions
    {
        public string BootstrapServers { get; set; }
        public int BatchSizeLimit { get; set; }
        public int Period { get; set; }
        public SecurityProtocol SecurityProtocol { get; set; }
        public SaslMechanism SaslMechanism { get; set; }
        public string Topic { get; set; }
        public string SaslUsername { get; set; }
        public string SaslPassword { get; set; }
        public string SslCaLocation { get; set; }
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