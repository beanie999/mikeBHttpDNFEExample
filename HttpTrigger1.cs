using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using Azure.Messaging.ServiceBus;

namespace NewRelic.Function;

public class HttpTrigger1
{
    private const string userParam = "user";
    private readonly ILogger _logger;

    public HttpTrigger1(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<HttpTrigger1>();
    }

    // New Relic helper function
    private NewRelic.Api.Agent.ITransaction helpNewRelic(HttpRequestData req)
    {
        // Ensure the transaction is named with the Url.
        NewRelic.Api.Agent.NewRelic.SetTransactionUri(req.Url);
        // Get the current transaction.
        NewRelic.Api.Agent.IAgent agent = NewRelic.Api.Agent.NewRelic.GetAgent();
        NewRelic.Api.Agent.ITransaction transaction = agent.CurrentTransaction;
        // Accept the trace headers if they were set by the caller.
        IEnumerable<string> Getter(HttpRequestData carrier, string key)
        {
            _logger.LogInformation($"Asking for key: {key}");
            return carrier.Headers.TryGetValues(key, out var values) ? new string[] { values.First() } : null;
        }
        transaction.AcceptDistributedTraceHeaders(req, Getter, NewRelic.Api.Agent.TransportType.HTTP);

        // Output the New Relic meta data to the log.
        NewRelic.Api.Agent.IAgent Agent = NewRelic.Api.Agent.NewRelic.GetAgent();
        var linkingMetadata = Agent.GetLinkingMetadata();
        foreach (KeyValuePair<string, string> kvp in linkingMetadata)
        {
            _logger.LogInformation($"New Relic meta data, Key = {kvp.Key}, Value = {kvp.Value}");
        }

        return transaction;
    }

    // Method to send messages to a Service Bus queue, with W3C trace headers (traced by New Relic).
    [NewRelic.Api.Agent.Trace]
    private async Task sendServiceBusMessage(string user)
    {

        // The client that owns the connection and can be used to create senders and receivers.
        ServiceBusClient client;
        // The sender used to publish messages to the queue.
        ServiceBusSender sender;

        // number of messages to be sent to the queue
        Random rnd = new Random();
        int numOfMessages = rnd.Next(5) + 1;
        // Connection parameters, will be read from the Azure Func Application Settings.
        string connectionString = System.Environment.GetEnvironmentVariable("SERVICE_BUS_CONNECTION", EnvironmentVariableTarget.Process);
        string queueName = System.Environment.GetEnvironmentVariable("SERVICE_BUS_QUEUE", EnvironmentVariableTarget.Process);

        // Set up the Service Bus objects.
        var clientOptions = new ServiceBusClientOptions()
        {
            TransportType = ServiceBusTransportType.AmqpWebSockets
        };
        client = new ServiceBusClient(connectionString, clientOptions);
        sender = client.CreateSender(queueName);

        // Create a batch. 
        using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

        // Set up the New Relic trace header objects, including the setter.
        NewRelic.Api.Agent.IAgent agent = NewRelic.Api.Agent.NewRelic.GetAgent();
        NewRelic.Api.Agent.ITransaction transaction = agent.CurrentTransaction;
        // The trace headers are added to the message as application properties.
        var setter = new Action<ServiceBusMessage, string, string>((message, key, value) => { message.ApplicationProperties.Add(key, value); });

        // Loop through the message adding them to the batch.
        for (int i = 1; i <= numOfMessages; i++)
        {
            // Try adding a message to the batch.
            ServiceBusMessage message = new ServiceBusMessage($"Message from user {user} number {i}.");
            // Insert the trace headers into the message application properties using the setter.
            transaction.InsertDistributedTraceHeaders(message, setter);
            if (!messageBatch.TryAddMessage(message))
            {
                // If it is too large for the batch.
                throw new Exception($"The message {i} is too large to fit in the batch.");
            }
        }

        try
        {
            // Use the producer client to send the batch of messages to the Service Bus queue.
            await sender.SendMessagesAsync(messageBatch);
            _logger.LogInformation($"A batch of {numOfMessages} messages has been published to the queue.");
        }
        finally
        {
            // Cleaned up objects.
            await sender.DisposeAsync();
            await client.DisposeAsync();
        }
    }

    // Method to call another Function App via HTTP (traced by New Relic).
    [NewRelic.Api.Agent.Trace]
    private HttpStatusCode callAppFunc(string user)
    {
        HttpClient client = new HttpClient();
        // Get the App Function URL from the Application settings.
        string URL = System.Environment.GetEnvironmentVariable("URL_TO_CALL", EnvironmentVariableTarget.Process);
        client.BaseAddress = new Uri(URL);
        // Build the URL parameters, we pass on the user name if it was supplied.
        string urlParameters = "?" + userParam + "=" + user;

        // List data response, blocking call to response or timeout.
        HttpResponseMessage response = client.GetAsync(urlParameters).Result;
        _logger.LogInformation($"Response was {response.StatusCode}");

        // Dispose once all HttpClient calls are complete.
        client.Dispose();
        return response.StatusCode;
    }

    // Main method, eclared as a New Relic web transaction.
    [Function("HttpTrigger1")]
    [NewRelic.Api.Agent.Transaction(Web = true)]
    public HttpResponseData Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req)
    {
        // Setup New Relic.
        NewRelic.Api.Agent.ITransaction transaction = helpNewRelic(req);
        _logger.LogInformation("mikeBHttpFEDNExample Function App called.");
        var responseCode = HttpStatusCode.OK;
        string responseText = "";
        // check for a valid user supplied parameter.
        if (req.Query.GetValues(userParam) != null && !String.IsNullOrEmpty(req.Query.GetValues(userParam).First()))
        {
            string user = req.Query.GetValues(userParam).First();
            _logger.LogInformation($"User is: {user}");
            // Set the userId in New Relic for Errors Inbox.
            transaction.SetUserId(user);
            responseText = $"Supplied with user: {user}";
            responseCode = this.callAppFunc(user);
            this.sendServiceBusMessage(user);
        }
        else
        {
            // Prompt the user to supply a user name as a parameter.
            _logger.LogError("No user supplied.");
            responseText = $"Please supply a user name via the user parameter. For example: {req.Url}?user=fred";
            responseCode = HttpStatusCode.BadRequest;
            // Tell New Relic there was an error!
            var errorAttributes = new Dictionary<string, string>{};
            NewRelic.Api.Agent.NewRelic.NoticeError("No user supplied.", errorAttributes);
        }

        // Build and return the response.
        var response = req.CreateResponse(responseCode);
        // Add the response code to New Relic.
        transaction.AddCustomAttribute("http.statusCode", responseCode);
        response.Headers.Add("Content-Type", "text/plain; charset=utf-8");
        response.WriteString(responseText);
        _logger.LogInformation("mikeBHttpFEDNExample Function App complete.");

        return response;
    }
}

