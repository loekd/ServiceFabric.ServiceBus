using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.ServiceFabric.Services.Communication.Runtime;

namespace ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners
{
    /// <summary>
    /// Implementation of <see cref="ICommunicationListener"/> that listens to a Service Bus Queue.
    /// </summary>
    public class GenericServiceBusCommunicationListener : ServiceBusCommunicationListenerBase
    {
        private readonly IReceiverClientFactory _clientFactory;

        private readonly IServiceBusMessageHandler _handler;

        /// <summary>
        /// Creates a new instance, using the init parameters of a <see cref="StatefulService"/>
        /// Uses external clientFactory to use msi or middleware
        /// </summary>
        /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
        /// <param name="clientFactory">A factory to create either QueueClient or SubscriptionClient to listen for messages</param>
        /// <param name="handler">A handler for incoming messages</param>
        public GenericServiceBusCommunicationListener(
            ServiceContext context,
            IReceiverClientFactory clientFactory,
            IServiceBusMessageHandler handler) :
            base(context)
        {
            _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
            _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        }

        /// <summary>
        /// Uses ext
        /// </summary>
        /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
        /// <param name="clientFactory">A factory to create either QueueClient or SubscriptionClient to listen for messages</param>
        /// <param name="receiverFactory">A factory to create handler for incoming messages</param>
        public GenericServiceBusCommunicationListener(
            ServiceContext context,
            IReceiverClientFactory clientFactory,
            IServiceBusMessageHandlerFactory receiverFactory
        ) :
            base(context)
        {
            _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
            _handler = receiverFactory?.Create(this) ?? throw new ArgumentNullException(nameof(receiverFactory));
        }
        /// <summary>
        /// Uses ext
        /// </summary>
        /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
        /// <param name="clientFactory">A factory to create either QueueClient or SubscriptionClient to listen for messages</param>
        /// <param name="receiverFactory">A factory to create handler for incoming messages</param>
        public GenericServiceBusCommunicationListener(
            ServiceContext context,
            IReceiverClientFactory clientFactory,
            Func<IServiceBusCommunicationListener, IServiceBusMessageHandler> receiverFactory
        ) :
            base(context)
        {
            _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
            _handler = receiverFactory?.Invoke(this) ?? throw new ArgumentNullException(nameof(receiverFactory));
        }

        /// <summary>
        /// Gets or sets the AutoRenewTimeout that will be passed to the <see cref="Handler"/>. Can be null.
        /// </summary>
        public TimeSpan? AutoRenewTimeout { get; set; }

        /// <summary>
        /// (Ignored when not using Sessions) Gets or sets the MaxConcurrentSessions that will be passed to the <see cref="Handler"/>. Can be null. 
        /// </summary>
        public int? MaxConcurrentSessions { get; set; }

        /// <summary>
        /// Gets the Service Bus Receiver client.
        /// </summary>
        protected IReceiverClient ReceiverClient { get; private set; }

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

            ReceiverClient.RegisterMessageHandler(ReceiveMessageAsync, options);
        }

        /// <inheritdoc />
        protected override Task<string> OpenImplAsync(CancellationToken cancellationToken)
        {
            ReceiverClient = _clientFactory.Create();
            if (MessagePrefetchCount > 0)
            {
                ReceiverClient.PrefetchCount = MessagePrefetchCount;
            }

            ListenForMessages();

            Thread.Yield();

            var uri = ReceiverClient.ServiceBusConnection?.Endpoint?.ToString();
            return Task.FromResult(uri);
        }


        /// <summary>
        /// Logs the error and continues.
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        protected virtual Task ExceptionReceivedHandler(ExceptionReceivedEventArgs args)
        {
            WriteLog($"There was an error while receiving a message: {args.Exception.Message}");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Will pass an incoming message to the <see cref="Handler"/> for processing.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        protected async Task ReceiveMessageAsync(Message message, CancellationToken cancellationToken)
        {
            try
            {
                ProcessingMessage.Wait(cancellationToken);
                var combined = CancellationTokenSource
                    .CreateLinkedTokenSource(cancellationToken, StopProcessingMessageToken).Token;
                await _handler.HandleAsync(message, combined);
            }
            finally
            {
                ProcessingMessage.Release();
            }
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
            await ReceiverClient.CloseAsync();
        }

        /// <inheritdoc />
        public override Task Complete(Message message)
        {
            return ReceiverClient.CompleteAsync(message.SystemProperties.LockToken);
        }

        /// <inheritdoc />
        public override Task Abandon(Message message, IDictionary<string, object> propertiesToModify = null)
        {
            return ReceiverClient.AbandonAsync(message.SystemProperties.LockToken, propertiesToModify);
        }

        /// <inheritdoc />
        public override Task DeadLetter(Message message, IDictionary<string, object> propertiesToModify = null)
        {
            return ReceiverClient.DeadLetterAsync(message.SystemProperties.LockToken, propertiesToModify);
        }
    }
}