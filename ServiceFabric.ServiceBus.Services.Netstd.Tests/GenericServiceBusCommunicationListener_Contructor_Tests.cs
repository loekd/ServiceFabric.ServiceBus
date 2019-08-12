using System;
using FakeItEasy;
using NUnit.Framework;
using ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners;

namespace ServiceFabric.ServiceBus.Services.Netstd.Tests
{
    public class GenericServiceBusCommunicationListener_Contructor_Tests
    {
        [Test]
        public void When_All_Parameters_Are_null_Throw_ArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new GenericServiceBusCommunicationListener(null, null, (IServiceBusMessageHandlerFactory) null));
            Assert.Throws<ArgumentNullException>(() => new GenericServiceBusCommunicationListener(null, null, (IServiceBusMessageReceiver) null));
        }
        
        [Test]
        public void When_ServiceBusMessageHandlerFactory_Is_null_Throw_ArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new GenericServiceBusCommunicationListener(null, null, A.Fake<IServiceBusMessageHandlerFactory>()));
        }
        
        [Test]
        public void When_ServiceBusMessageHandler_Is_null_Throw_ArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new GenericServiceBusCommunicationListener(null, null, A.Fake<IServiceBusMessageReceiver>()));
        }
        
        
        [Test]
        public void When_ServiceBusMessageHandlerFunc_Is_null_Throw_ArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new GenericServiceBusCommunicationListener(null, null, (s) => A.Fake<IServiceBusMessageReceiver>()));
        }
        
        [Test]
        public void When_ServiceBusMessageHandlerFactory_Create_return_Is_null_Throw_ArgumentNullException()
        {
            var factory = A.Fake<IServiceBusMessageHandlerFactory>();
            A.CallTo(() => factory.Create(A<IServiceBusCommunicationListener>._))
                .Returns(null);
            Assert.Throws<ArgumentNullException>(() => new GenericServiceBusCommunicationListener(null, null,factory));
        }
        
        [Test]
        public void When_ServiceBusMessageHandlerFunc_return_Is_null_Throw_ArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new GenericServiceBusCommunicationListener(null, null, (s) => null));
        }
        
        [Test]
        public void When_Only_Context_Is_Null_Throws_Nothing()
        {
            var handler = A.Fake<IServiceBusMessageReceiver>();
            var factory = A.Fake<IServiceBusMessageHandlerFactory>();
            var clientFactory = A.Fake<IReceiverClientFactory>();
            Assert.DoesNotThrow(() => new GenericServiceBusCommunicationListener(null, clientFactory, handler));
            Assert.DoesNotThrow(() => new GenericServiceBusCommunicationListener(null, clientFactory, factory));
            Assert.DoesNotThrow(() => new GenericServiceBusCommunicationListener(null, clientFactory, (s) => handler));
        }
    }
}