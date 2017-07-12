using System;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace ServiceFabric.ServiceBus.Services.CommunicationListeners
{
    /// <summary>
    /// Base implementation of <see cref="ICommunicationListener"/> that listens to a Service Bus Subscriptions.
    /// </summary>
    public abstract class ServiceBusSubscriptionCommunicationListenerBase : ServiceBusCommunicationListener
    {
        /// <summary>
        /// Gets the Service Bus Subscription client.
        /// </summary>
        protected SubscriptionClient ServiceBusClient { get; private set; }

        /// <summary>
        /// Gets the name of the monitored Service Bus Topic.
        /// </summary>
        [Obsolete("Replaced by TopicName")]
        protected string ServiceBusTopicName => TopicName;

        /// <summary>
        /// Gets the name of the monitored Service Bus Topic.
        /// </summary>
        protected string TopicName { get; }

        /// <summary>
        /// Gets the name of the monitored Service Bus Topic Subscription.
        /// </summary>
        [Obsolete("Replaced by SubscriptionName")]
        protected string ServiceBusSubscriptionName => SubscriptionName;

        /// <summary>
        /// Gets the name of the monitored Service Bus Topic Subscription.
        /// </summary>
        protected string SubscriptionName { get; }

        /// <summary>
        /// Creates a new instance, using the init parameters of a <see cref="StatefulService"/>
        /// </summary>
        /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
        /// <param name="serviceBusTopicName">The name of the monitored Service Bus Topic (optional, EntityPath is supported too)</param>
        /// <param name="serviceBusSubscriptionName">The name of the monitored Service Bus Topic Subscription</param>
        /// <param name="serviceBusSendConnectionString">(Optional) A Service Bus connection string that can be used for Sending messages. 
        /// (Returned as Service Endpoint.) When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Receive'
        ///  is used.</param>
        /// <param name="serviceBusReceiveConnectionString">(Optional) A Service Bus connection string that can be used for Receiving messages. 
        ///  When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Receive'
        ///  is used.</param>
        /// <param name="requireSessions">Indicates whether the provided Message Queue requires sessions.</param>
        protected ServiceBusSubscriptionCommunicationListenerBase(ServiceContext context, string serviceBusTopicName, string serviceBusSubscriptionName, string serviceBusSendConnectionString = null, string serviceBusReceiveConnectionString = null, bool requireSessions = false)
            : base(context, serviceBusSendConnectionString, serviceBusReceiveConnectionString, requireSessions)
        {
            if (string.IsNullOrWhiteSpace(serviceBusSubscriptionName)) throw new ArgumentOutOfRangeException(nameof(serviceBusSubscriptionName));
            if (string.IsNullOrWhiteSpace(serviceBusTopicName)) serviceBusTopicName = null;

            var builder = new ServiceBusConnectionStringBuilder(ReceiveConnectionString);
            string entityPath = builder.EntityPath;
           
            if ((!string.IsNullOrWhiteSpace(serviceBusTopicName) && !string.IsNullOrWhiteSpace(entityPath)) || string.IsNullOrWhiteSpace(serviceBusTopicName) && string.IsNullOrWhiteSpace(entityPath))
                throw new ArgumentException(nameof(serviceBusTopicName), $"Please provide either {nameof(serviceBusTopicName)} or a receive connection string with an entitypath.");
            
            TopicName = serviceBusTopicName ?? entityPath;
            SubscriptionName = serviceBusSubscriptionName;
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
            //use receive url:
            ServiceBusClient = SubscriptionClient.CreateFromConnectionString(ReceiveConnectionString, TopicName, SubscriptionName, ReceiveMode);
            if (MessagePrefetchCount > 0)
            {
                ServiceBusClient.PrefetchCount = MessagePrefetchCount;
            }

            if (RequireSessions)
            {
                ListenForSessionMessages();
            }
            else
            {
                ListenForMessages();
            }
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


        /// <summary>
        /// Starts listening for session messages on the configured Service Bus Queue.
        /// </summary>
        protected abstract void ListenForSessionMessages();


    }
}