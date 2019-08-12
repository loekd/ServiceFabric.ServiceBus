using ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners;

namespace ServiceFabric.ServiceBus.Services.Netstd
{
    public interface IServiceBusMessageHandlerFactory
    {
        IServiceBusMessageReceiver Create(IServiceBusCommunicationListener listener);
    }
}