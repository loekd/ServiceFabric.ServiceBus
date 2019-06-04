using ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners;

namespace ServiceFabric.ServiceBus.Services.Netstd
{
    public interface IServiceBusMessageHandlerFactory
    {
        IServiceBusMessageHandler Create(IServiceBusCommunicationListener listener);
    }
}