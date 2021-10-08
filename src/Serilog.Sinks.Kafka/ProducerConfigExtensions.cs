using Confluent.Kafka;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Serilog.Sinks.Kafka
{
    public static class ProducerConfigExtensions
    {
        private const string SerilogEnvVar = "SERILOG__KAFKA__";

        public static ProducerConfig LoadFromEnvironmentVariables(this ProducerConfig config)
        {
            var envVars = Environment.GetEnvironmentVariables();

            foreach (DictionaryEntry envVar in envVars)
            {
                var key = envVar.Key.ToString();
                var value = envVar.Value.ToString();

                if (key.StartsWith(SerilogEnvVar))
                {
                    var configItem = key
                        .Replace(SerilogEnvVar, string.Empty)
                        .Replace("_", "")
                        .ToLower();

                    config.SetValue(configItem, value);
                }
            }

            return config;
        }

        public static ProducerConfig SetValue(this ProducerConfig config, string key, object value)
        {
            SetValues(config, key, value?.ToString());

            return config;
        }

        private static void SetValues(object obj, string propertyName, string stringValue)
        {
            if (string.IsNullOrEmpty(stringValue))
                return;

            var propertyInfo = obj.GetType().
                GetProperties()
                .SingleOrDefault(x => x.Name.ToLower() == propertyName.ToLower());

            object objValue = null;

            if (propertyInfo == null)
                throw new ArgumentException($"A property ({propertyName}) could not be found in Confluent.Kafka)");

            var convertValue = new Dictionary<Type, Action>
            {
                { typeof(string), () => objValue = stringValue },
                { typeof(int?), () => objValue = int.Parse(stringValue) },
                { typeof(bool?), () => objValue = bool.Parse(stringValue) },
                { typeof(Partitioner?), () => objValue = Enum.Parse(typeof(Partitioner), stringValue) },
                { typeof(CompressionType?), () => objValue = Enum.Parse(typeof(CompressionType), stringValue) },
                { typeof(SecurityProtocol?), () => objValue = Enum.Parse(typeof(SecurityProtocol), stringValue) },
                { typeof(SaslMechanism?), () => objValue = Enum.Parse(typeof(SaslMechanism), stringValue) },
                { typeof(BrokerAddressFamily?), () => objValue = Enum.Parse(typeof(BrokerAddressFamily), stringValue) },
                { typeof(Acks?), () => objValue = Enum.Parse(typeof(Acks), stringValue) },
                { typeof(SslEndpointIdentificationAlgorithm?), () => objValue = Enum.Parse(typeof(SslEndpointIdentificationAlgorithm), stringValue) }
            };

            convertValue[propertyInfo.PropertyType]();

            if (objValue == null)
                throw new InvalidCastException($"{stringValue} could not be assigned to {propertyName} ({propertyInfo.PropertyType.Name})");

            propertyInfo.SetValue(obj, objValue);
        }
    }
}
