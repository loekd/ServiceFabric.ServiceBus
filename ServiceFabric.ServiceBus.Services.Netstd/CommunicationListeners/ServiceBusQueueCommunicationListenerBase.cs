using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners
{
    /// <summary>
    /// Base implementation of <see cref="ICommunicationListener"/> that listens to a Service Bus Queue.
    /// </summary>
    public abstract class ServiceBusQueueCommunicationListenerBase : ServiceBusCommunicationListener
    {
        /// <summary>
        /// Gets the Service Bus Queue client.
        /// </summary>
        protected QueueClient ServiceBusClient { get; private set; }

        /// <summary>
        /// Gets the name of the monitored Service Bus Queue.
        /// </summary>
        protected string QueueName { get; }

        /// <summary>
        /// Creates a new instance, using the init parameters of a <see cref="StatefulService"/>
        /// </summary>
        /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
        /// <param name="serviceBusQueueName">(Optional) The name of the monitored Service Bus Queue (EntityPath in connectionstring is supported too)</param>
        /// <param name="serviceBusSendConnectionString">(Optional) A Service Bus connection string that can be used for Sending messages. (Returned as Service Endpoint.) 
        /// </param>
        /// <param name="serviceBusReceiveConnectionString">(Required) A Service Bus connection string that can be used for Receiving messages. 
        /// </param>
        protected ServiceBusQueueCommunicationListenerBase(ServiceContext context, string serviceBusQueueName,
            string serviceBusSendConnectionString,
            string serviceBusReceiveConnectionString)
            : base(context, serviceBusSendConnectionString, serviceBusReceiveConnectionString)
        {
            var builder = new ServiceBusConnectionStringBuilder(ReceiveConnectionString);
            if ((!string.IsNullOrWhiteSpace(serviceBusQueueName) && !string.IsNullOrWhiteSpace(builder.EntityPath)) || string.IsNullOrWhiteSpace(serviceBusQueueName) && string.IsNullOrWhiteSpace(builder.EntityPath))
                throw new ArgumentException(nameof(serviceBusQueueName), $"Please provide either {nameof(serviceBusQueueName)} or a receive connection string with an entitypath.");

            QueueName = serviceBusQueueName;
        }

        /// <summary>
        /// This method causes the communication listener to be opened. Once the Open
        ///             completes, the communication listener becomes usable - accepts and sends messages.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation. The result of the Task is
        ///             the endpoint string.
        /// </returns>
        public override Task<string> OpenAsync(CancellationToken cancellationToken)
        {
            var builder = new ServiceBusConnectionStringBuilder(ReceiveConnectionString);
            if (string.IsNullOrWhiteSpace(builder.EntityPath))
            {
                builder.EntityPath = QueueName;
            }

            ServiceBusClient = new QueueClient(builder, ReceiveMode, RetryPolicy);

            if (MessagePrefetchCount > 0)
            {
                ServiceBusClient.PrefetchCount = MessagePrefetchCount;
            }

            ListenForMessages();

            Thread.Yield();

            //create send url:
            string uri = SendConnectionString;
            return Task.FromResult(uri);
        }

        /// <summary>
        /// This method causes the communication listener to close. Close is a terminal state and 
        ///             this method allows the communication listener to transition to this state in a
        ///             graceful manner.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation.
        /// </returns>
        protected override async Task CloseImplAsync(CancellationToken cancellationToken)
        {
            await ServiceBusClient.CloseAsync();
        }

        /// <summary>
        /// Starts listening for messages on the configured Service Bus Queue.
        /// </summary>
        protected abstract void ListenForMessages();

        /// <inheritdoc />
        public override Task Complete(Message message)
        {
            return ServiceBusClient.CompleteAsync(message.SystemProperties.LockToken);
        }

        /// <inheritdoc />
        public override Task Abandon(Message message, IDictionary<string, object> propertiesToModify = null)
        {
            return ServiceBusClient.AbandonAsync(message.SystemProperties.LockToken, propertiesToModify);
        }

        /// <inheritdoc />
        public override Task DeadLetter(Message message, IDictionary<string, object> propertiesToModify = null)
        {
            return ServiceBusClient.DeadLetterAsync(message.SystemProperties.LockToken, propertiesToModify);
        }
    }
}