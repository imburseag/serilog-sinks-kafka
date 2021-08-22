using System;

namespace Serilog.Sinks.Kafka
{
    /// <summary>
    /// Sepecifies options for handling failures when emitting the events to Elasticsearch. Can be a combination of options.
    /// </summary>
    [Flags]
    public enum EmitEventFailureHandling
    {
        /// <summary>
        /// Send the error to the SelfLog
        /// </summary>
        WriteToSelfLog = 1,

        /// <summary>
        /// Write the events to another sink. Make sure to configure this one.
        /// </summary>
        WriteToFailureSink = 2,

        /// <summary>
        /// Throw the exception to the caller.
        /// </summary>
        ThrowException = 4,

        /// <summary>
        /// The failure callback function will be called when the event cannot be submitted to Elasticsearch.
        /// </summary>
        RaiseCallback = 8
    }
}