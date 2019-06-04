using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FakeItEasy;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using NUnit.Framework;
using ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners;

namespace ServiceFabric.ServiceBus.Services.Netstd.Tests
{
    [TestFixture]
    public class GenericServiceBusCommunicationListener_HandlerFactory_Tests
    {
        private IServiceBusMessageHandlerFactory _handlerFactory;
        private IServiceBusMessageHandler _handler;
        private IReceiverClientFactory _factory;
        private GenericServiceBusCommunicationListener _sut;
        private IReceiverClient _receiverClient;

        [SetUp]
        public void SetUp()
        {
            _handlerFactory = A.Fake<IServiceBusMessageHandlerFactory>();
            _handler = A.Fake<IServiceBusMessageHandler>();
            A.CallTo(() => _handlerFactory.Create(A<IServiceBusCommunicationListener>._))
                .Returns(_handler);
            _factory = A.Fake<IReceiverClientFactory>();
            _receiverClient = A.Fake<IReceiverClient>();
            A.CallTo(() => _factory.Create())
                .Returns(_receiverClient);
            _sut = new GenericServiceBusCommunicationListener(null, _factory, _handlerFactory);
        }

        [Test]
        public async Task When_Listener_Is_Opened_Register_MessageHandler()
        {
            await _sut.OpenAsync(CancellationToken.None);
            A.CallTo(() =>
                    _receiverClient.RegisterMessageHandler(
                        A<Func<Message, CancellationToken, Task>>._,
                        A<MessageHandlerOptions>._))
                .MustHaveHappenedOnceExactly();
        }

        [Test]
        public async Task When_Listener_Is_Opened_Correct_MaxConcurrentCalls()
        {
            var expected = 42;
            _sut.MaxConcurrentCalls = expected;

            await _sut.OpenAsync(CancellationToken.None);

            AssertMessageHandlerOptions(options => options.MaxConcurrentCalls == expected);
        }

        [Test]
        public async Task When_Listener_Is_Opened_Correct_MaxAutoRenewDuration()
        {
            var expected = TimeSpan.FromMinutes(42);
            _sut.AutoRenewTimeout = expected;

            await _sut.OpenAsync(CancellationToken.None);

            AssertMessageHandlerOptions(options => options.MaxAutoRenewDuration == expected);
        }

        [Test]
        public async Task When_Listener_Is_CreateClient_with_Correct_MessagePrefetchCount()
        {
            var expected = 42;
            _sut.MessagePrefetchCount = expected;

            await _sut.OpenAsync(CancellationToken.None);

            A.CallToSet((() => _receiverClient.PrefetchCount))
                .To(expected)
                .MustHaveHappenedOnceExactly();
        }

        [Test]
        public async Task When_Listener_Is_Closed_Calls_Close_On_ReceiverClient()
        {
            var expected = 42;
            _sut.MessagePrefetchCount = expected;

            await _sut.OpenAsync(CancellationToken.None);
            await _sut.CloseAsync(CancellationToken.None);

            A.CallTo((() => _receiverClient.CloseAsync()))
                .MustHaveHappenedOnceExactly();
        }

        [Test]
        public async Task When_Message_Triggered_Send_To_Handler()
        {
            //Dummy message
            var message = new Message();

            // Get added handler to ReceiverClient
            Func<Message, CancellationToken, Task> handler = null;
            A.CallTo(() =>
                    _receiverClient.RegisterMessageHandler(
                        A<Func<Message, CancellationToken, Task>>._,
                        A<MessageHandlerOptions>._))
                .Invokes(call => { handler = call.GetArgument<Func<Message, CancellationToken, Task>>(0); });

            await _sut.OpenAsync(CancellationToken.None);

            //Send message from ReceiverClient
            await handler(message, CancellationToken.None);

            // Assert that added message handler gets triggered on message
            A.CallTo(() => _handler.HandleAsync(A<Message>.That.IsSameAs(message), A<CancellationToken>._))
                .MustHaveHappenedOnceExactly();
        }

        [Test]
        public async Task When_Listener_Is_Create_Handler_Factory_And_Inject_It_Self()
        {
            await _sut.OpenAsync(CancellationToken.None);

            A.CallTo(() => _handlerFactory.Create(A<IServiceBusCommunicationListener>.That.IsSameAs(_sut)))
                .MustHaveHappenedOnceExactly();
        }


        [Test]
        public async Task When_DeadLetter_Message_Delegate_LockToken_To_ReceiverClient_DeadLetterAsync()
        {
            //Dummy message
            var message = CreateFakeMessage();

            await _sut.OpenAsync(CancellationToken.None);
            await _sut.DeadLetter(message);

            A.CallTo(() =>
                    _receiverClient.DeadLetterAsync(message.SystemProperties.LockToken,
                        A<IDictionary<string, object>>._))
                .MustHaveHappenedOnceExactly();
        }

        [Test]
        public async Task When_Abandon_Message_Delegate_LockToken_To_ReceiverClient_AbandonAsync()
        {
            //Dummy message
            var message = CreateFakeMessage();

            await _sut.OpenAsync(CancellationToken.None);
            await _sut.Abandon(message);

            A.CallTo(() =>
                    _receiverClient.AbandonAsync(message.SystemProperties.LockToken,
                        A<IDictionary<string, object>>._))
                .MustHaveHappenedOnceExactly();
        }

        [Test]
        public async Task When_Complete_Message_Delegate_LockToken_To_ReceiverClient_ComplateAsync()
        {
            //Dummy message
            var message = CreateFakeMessage();

            await _sut.OpenAsync(CancellationToken.None);
            await _sut.Complete(message);

            A.CallTo(() => _receiverClient.CompleteAsync(message.SystemProperties.LockToken))
                .MustHaveHappenedOnceExactly();
        }

        private static Message CreateFakeMessage()
        {
            var message = new Message();
            typeof(Message.SystemPropertiesCollection)
                .GetField("sequenceNumber",
                    System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                .SetValue(message.SystemProperties, 1);
            return message;
        }

        private void AssertMessageHandlerOptions(Func<MessageHandlerOptions, bool> predicate)
        {
            A.CallTo(() =>
                    _receiverClient.RegisterMessageHandler(
                        A<Func<Message, CancellationToken, Task>>._,
                        A<MessageHandlerOptions>._))
                .WhenArgumentsMatch(args =>
                {
                    var handlerOptions = args.Get<MessageHandlerOptions>(1);
                    return predicate(handlerOptions);
                })
                .MustHaveHappenedOnceExactly();
        }
    }
}