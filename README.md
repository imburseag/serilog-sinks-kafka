# serilog-sinks-kafka

[![CircleCI](https://circleci.com/gh/jonhoare/serilog-sinks-kafka/tree/master.svg?style=svg)](https://circleci.com/gh/jonhoare/serilog-sinks-kafka/tree/master)

A Serilog sink that writes events to Kafka Endpoints (Including Azure Event Hubs).

## Usage

```
Log.Logger = new LoggerConfiguration()
    .WriteTo.Kafka()
    .CreateLogger();
```

### Parameters
* bootstrapServers - Comma separated list of Kafka Bootstrap Servers. Defaults to "localhost:9092"
* batchSizeLimit - Maximum number of logs to batch. Defaults to 50
* period - The period in seconds to send batches of logs. Defaults to 5 seconds
* securityProtocol -  SecurityProtocol.Plaintext
* saslMechanism - The SASL Mecahnism. Defaults to SaslMechanism.Plain
* topic - Name of the Kafka topic. Defaults to "logs"
* saslUsername - (Optional) Username for SASL. This is required for Azure Event Hubs and should be set to `$ConnectionString`
* saslPassword - (Optional) Password for SASL. This is required for Azure Event Hubs and is your entire Connection String.
* sslCaLocation - (Optional) Location of the SSL CA Certificates This is required for Azure Event Hubs and should be set to `./cacert.pem` as this package includes the Azure carcert.pem file which is copied into your binary output directory.


## Configuration for a local Kafka instance

```
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Debug",
      "Override": {
        "Microsoft": "Warning",
        "System": "Warning"
      }
    },
    "WriteTo": [
      {
        "Name": "Kafka",
        "Args": {
          "batchSizeLimit": "50",
          "period": "5",
          "bootstrapServers": "localhost:9092",
          "topic": "logs"
        }
      }
    ]
  }
}

```

Can also be configured to be used with Azure Event Hubs

## Configuration for Azure Event Hubs instance

A copy of the CA Certificates for Azure are included in this package and will be copied to your output directory as `cacert.pem`

```
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Debug",
      "Override": {
        "Microsoft": "Warning",
        "System": "Warning"
      }
    },
    "WriteTo": [
      {
        "Name": "Kafka",
        "Args": {
          "batchSizeLimit": "50",
          "period": "5",
          "bootstrapServers": "my-event-hub-instance.servicebus.windows.net:9093",
          "saslUsername": "$ConnectionString",
          "saslPassword": "my-event-hub-instance-connection-string",
          "topic": "logs",
          "sslCaLocation": "./cacert.pem",
          "saslMechanism": "Plain",
          "securityProtocol": "SaslSsl"
        }
      }
    ]
  }
}

```