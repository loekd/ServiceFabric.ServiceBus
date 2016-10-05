using System;
using System.Fabric;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace ServiceFabric.ServiceBus.Services.CommunicationListeners
{
    /// <summary>
    /// Implementation of <see cref="ICommunicationListener"/> that listens to a Service Bus Queue.
    /// </summary>
    public class ServiceBusSubscriptionCommunicationListener : ServiceBusSubscriptionCommunicationListenerBase
    {
        /// <summary>
        /// Gets or sets the AutoRenewTimeout that will be passed to the <see cref="Receiver"/>. Can be null.
        /// </summary>
        public TimeSpan? AutoRenewTimeout { get; set; }

        /// <summary>
        /// (Ignored when using Sessions) Gets or sets the MaxConcurrentCalls that will be passed to the <see cref="Receiver"/>. Can be null. 
        /// </summary>
        public int? MaxConcurrentCalls { get; set; }

        /// <summary>
        /// (Ignored when not using Sessions) Gets or sets the MaxConcurrentSessions that will be passed to the <see cref="Receiver"/>. Can be null. 
        /// </summary>
        public int? MaxConcurrentSessions { get; set; }

        /// <summary>
        /// Processor for messages.
        /// </summary>
        protected IServiceBusMessageReceiver Receiver { get; }

        /// <summary>
        /// Creates a new instance, using the init parameters of a <see cref="StatefulService"/>
        /// </summary>
        /// <param name="receiver">(Required) Processes incoming messages.</param>
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
        public ServiceBusSubscriptionCommunicationListener(IServiceBusMessageReceiver receiver, ServiceContext context, string serviceBusTopicName, string serviceBusSubscriptionName, string serviceBusSendConnectionString = null, string serviceBusReceiveConnectionString = null, bool requireSessions = false)
            : base(context, serviceBusTopicName, serviceBusSubscriptionName, serviceBusSendConnectionString, serviceBusReceiveConnectionString, requireSessions)
        {
            Receiver = receiver;
        }

        /// <summary>
        /// Starts listening for messages on the configured Service Bus Queue.
        /// </summary>
        protected override void ListenForMessages()
        {
            var options = new OnMessageOptions();
            if (AutoRenewTimeout.HasValue)
            {
                options.AutoRenewTimeout = AutoRenewTimeout.Value;
            }
            if (MaxConcurrentCalls.HasValue)
            {
                options.MaxConcurrentCalls = MaxConcurrentCalls.Value;
            }
            ServiceBusClient.OnMessageAsync(message => ReceiveMessageAsync(message, null), options);
        }

        /// <summary>
        /// Starts listening for session messages on the configured Service Bus subscription.
        /// </summary>
        protected override void ListenForSessionMessages()
        {
            var options = new SessionHandlerOptions();
            if (AutoRenewTimeout.HasValue)
            {
                options.AutoRenewTimeout = AutoRenewTimeout.Value;
            }
            if (MaxConcurrentSessions.HasValue)
            {
                options.MaxConcurrentSessions = MaxConcurrentSessions.Value;
            }
            ServiceBusClient.RegisterSessionHandlerFactory(new SessionHandlerFactory(this), options);
        }


        private class SessionHandlerFactory : IMessageSessionAsyncHandlerFactory
        {
            private readonly ServiceBusSubscriptionCommunicationListener _listener;

            public SessionHandlerFactory(ServiceBusSubscriptionCommunicationListener listener)
            {
                _listener = listener;
            }

            public IMessageSessionAsyncHandler CreateInstance(MessageSession session, BrokeredMessage message)
            {
                return new SessionHandler(_listener);
            }

            public void DisposeInstance(IMessageSessionAsyncHandler handler)
            {
            }
        }

        private class SessionHandler : MessageSessionAsyncHandler
        {
            private readonly ServiceBusSubscriptionCommunicationListener _listener;

            public SessionHandler(ServiceBusSubscriptionCommunicationListener listener)
            {
                _listener = listener;
            }
            protected override Task OnMessageAsync(MessageSession session, BrokeredMessage message)
            {
                return _listener.ReceiveMessageAsync(message, session);
            }
        }

        /// <summary>
        /// Will pass an incoming message to the <see cref="Receiver"/> for processing.
        /// </summary>
        /// <param name="messageSession">Contains the MessageSession when sessions are enabled.</param>
        /// <param name="message"></param>
        protected async Task ReceiveMessageAsync(BrokeredMessage message, MessageSession messageSession)
        {
            try
            {
                ProcessingMessage.Reset();
                await Receiver.ReceiveMessageAsync(message, messageSession, StopProcessingMessageToken);
                if (Receiver.AutoComplete)
                {
                    await message.CompleteAsync();
                }
            }
            finally
            {
                message.Dispose();
                ProcessingMessage.Set();
            }
        }
    }
}
