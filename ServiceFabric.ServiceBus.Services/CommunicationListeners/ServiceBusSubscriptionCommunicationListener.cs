using System;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using System.Collections.Generic;

namespace ServiceFabric.ServiceBus.Services.CommunicationListeners
{
    /// <summary>
    /// Implementation of <see cref="ICommunicationListener"/> that listens to a Service Bus SubscriptionS.
    /// </summary>
    public class ServiceBusSubscriptionCommunicationListener : ServiceBusCommunicationListener
    {
        private SubscriptionClient _serviceBusClient;

        /// <summary>
        /// Gets the name of the monitored Service Bus Topic.
        /// </summary>
        protected string ServiceBusTopicName { get; }

        /// <summary>
        /// Gets the name of the monitored Service Bus Topic Subscription.
        /// </summary>
        protected string ServiceBusSubscriptionName { get; }

        /// <summary>
        /// Creates a new instance, using the init parameters of a <see cref="StatefulService"/>
        /// </summary>
        /// <param name="receiver">(Required) Processes incoming messages.</param>
        /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
        /// <param name="serviceBusTopicName">The name of the monitored Service Bus Topic</param>
        /// <param name="serviceBusSubscriptionName">The name of the monitored Service Bus Topic Subscription</param>
        /// <param name="serviceBusSendConnectionString">(Optional) A Service Bus connection string that can be used for Sending messages. 
        /// (Returned as Service Endpoint.) When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Receive'
        ///  is used.</param>
        /// <param name="serviceBusReceiveConnectionString">(Optional) A Service Bus connection string that can be used for Receiving messages. 
        ///  When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Receive'
        ///  is used.</param>
        public ServiceBusSubscriptionCommunicationListener(IServiceBusMessageReceiver receiver, ServiceContext context, string serviceBusTopicName, string serviceBusSubscriptionName, string serviceBusSendConnectionString = null, string serviceBusReceiveConnectionString = null)
            : base(receiver, context, serviceBusSendConnectionString, serviceBusReceiveConnectionString)
        {
            if (string.IsNullOrWhiteSpace(serviceBusTopicName)) throw new ArgumentOutOfRangeException(nameof(serviceBusTopicName));
            if (string.IsNullOrWhiteSpace(serviceBusSubscriptionName)) throw new ArgumentOutOfRangeException(nameof(serviceBusSubscriptionName));

            ServiceBusTopicName = serviceBusTopicName;
            ServiceBusSubscriptionName = serviceBusSubscriptionName;
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
        public override async Task<string> OpenAsync(CancellationToken cancellationToken)
        {
            //use receive url:
            _serviceBusClient = SubscriptionClient.CreateFromConnectionString(ServiceBusReceiveConnectionString, ServiceBusTopicName,
                ServiceBusSubscriptionName, ServiceBusReceiveMode);
            if (ServiceBusMessagePrefetchCount > 0)
            {
                _serviceBusClient.PrefetchCount = ServiceBusMessagePrefetchCount;
            }
            bool requiresSession = await RequiresSession();

            if (requiresSession)
            {
                ListenForSessionMessages();
            }
            else
            {
                ListenForMessages();
            }
            Thread.Yield();

            //create send url:
            string uri = ServiceBusSendConnectionString;
            return uri;
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
            await _serviceBusClient.CloseAsync();
        }

        /// <summary>
        /// Starts listening for messages on the configured Service Bus Subscription.
        /// </summary>
        private void ListenForMessages()
        {
            ThreadStart ts = async () =>
            {
                while (!StopProcessingMessageToken.IsCancellationRequested)
                {
                    var messages = _serviceBusClient.ReceiveBatch(ServiceBusMessageBatchSize, ServiceBusServerTimeout);
                    foreach (var message in messages)
                    {
                        if (StopProcessingMessageToken.IsCancellationRequested) break;
                        await ReceiveMessageAsync(message);
                    }
                }
            };
            StartBackgroundThread(ts);
        }

        /// <summary>
		/// Starts listening for session messages on the configured Service Bus Subscription.
		/// </summary>
        private void ListenForSessionMessages()
        {
            ThreadStart ts = async () =>
            {
                while (!StopProcessingMessageToken.IsCancellationRequested)
                {
                    IEnumerable<BrokeredMessage> messages = null;
                    try
                    {
                        var session = _serviceBusClient.AcceptMessageSession(ServiceBusServerTimeout);

                        do
                        {
                            messages = session.ReceiveBatch(ServiceBusMessageBatchSize, ServiceBusServerTimeout);
                            foreach (var message in messages)
                            {
                                if (StopProcessingMessageToken.IsCancellationRequested) break;
                                await ReceiveMessageAsync(message);
                            }
                        }
                        while (messages != null);
                        session.Close();
                    }
                    catch (TimeoutException)
                    { }
                }
            };
            StartBackgroundThread(ts);
        }

        /// <summary>
        /// Indicates whether the Subscription is configured to require session use.
        /// </summary>
        /// <returns></returns>
        private async Task<bool> RequiresSession()
        {
            var namespaceMgr = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(ServiceBusReceiveConnectionString);
            var queue = await namespaceMgr.GetSubscriptionAsync(ServiceBusTopicName, ServiceBusSubscriptionName);
            return queue.RequiresSession;
        }
    }
}