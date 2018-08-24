using System;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners
{
    /// <summary>
    /// Implementation of <see cref="ICommunicationListener"/> that listens to a Service Bus Queue.
    /// </summary>
    public class ServiceBusQueueCommunicationListener : ServiceBusQueueCommunicationListenerBase
    {
        /// <summary>Gets or sets the maximum duration within which the lock will be renewed automatically. This
        /// value should be greater than the longest message lock duration; for example, the LockDuration Property. </summary>
        /// <value>The maximum duration during which locks are automatically renewed.</value>
        public TimeSpan? AutoRenewTimeout { get; set; }

        /// <summary>
        /// (Ignored when using Sessions) Gets or sets the MaxConcurrentCalls that will be passed to the <see cref="Receiver"/>. Can be null. 
        /// </summary>
        public int? MaxConcurrentCalls { get; set; }

        /// <summary>
        /// Processor for messages.
        /// </summary>
        protected IServiceBusMessageReceiver Receiver { get; }

        /// <summary>
        /// Creates a new instance, using the init parameters of a <see cref="StatefulService"/>
        /// </summary>
        /// <param name="receiver">(Required) Processes incoming messages.</param>
        /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
        /// <param name="serviceBusQueueName">(Optional) The name of the monitored Service Bus Queue (EntityPath in connectionstring is supported too)</param>
        /// <param name="serviceBusSendConnectionString">(Optional) A Service Bus connection string that can be used for Sending messages. 
        /// (Returned as Service Endpoint.).
        /// </param>
        /// <param name="serviceBusReceiveConnectionString">(Required) A Service Bus connection string that can be used for Receiving messages. 
        /// </param>
        public ServiceBusQueueCommunicationListener(IServiceBusMessageReceiver receiver, 
            ServiceContext context, 
            string serviceBusQueueName, 
            string serviceBusSendConnectionString, 
            string serviceBusReceiveConnectionString)
            : base(context, serviceBusQueueName, serviceBusSendConnectionString, serviceBusReceiveConnectionString)
        {
            Receiver = receiver;
        }

        /// <summary>
        /// Starts listening for messages on the configured Service Bus Queue.
        /// </summary>
        protected override void ListenForMessages()
        {
            var options = new MessageHandlerOptions(ExceptionReceivedHandler);
            if (AutoRenewTimeout.HasValue)
            {
                options.MaxAutoRenewDuration = AutoRenewTimeout.Value;
            }
            if (MaxConcurrentCalls.HasValue)
            {
                options.MaxConcurrentCalls = MaxConcurrentCalls.Value;
            }
            ServiceBusClient.RegisterMessageHandler(ReceiveMessageAsync, options);
        }

        /// <summary>
        /// Logs the error and continues.
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        protected virtual Task ExceptionReceivedHandler(ExceptionReceivedEventArgs args)
        {
            LogAction($"There was an error while receiving a message: {args.Exception.Message}");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Will pass an incoming message to the <see cref="Receiver"/> for processing.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        protected async Task ReceiveMessageAsync(Message message, CancellationToken cancellationToken)
        {
            try
            {
                ProcessingMessage.Reset();
                var combined = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, StopProcessingMessageToken).Token;
                await Receiver.ReceiveMessageAsync(message, combined);
                if (Receiver.AutoComplete)
                {
                    await ServiceBusClient.CompleteAsync(message.SystemProperties.LockToken);
                }
            }
            finally
            {
                ProcessingMessage.Set();
            }
        }
    }
}