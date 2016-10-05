using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.ServiceBus.Messaging;

namespace ServiceFabric.ServiceBus.Services.CommunicationListeners
{
    /// <summary>
    /// Holds a set of <see cref="MessageLockRenewTimer"/> instances for every provided <see cref="BrokeredMessage"/>.
    /// </summary>
    public sealed class MessageLockRenewTimerSet : IDisposable
    {
        private readonly Dictionary<BrokeredMessage, MessageLockRenewTimer> _timers;

        /// <summary>
        /// Returns only Dummy disposables from this[get].
        /// </summary>
        public MessageLockRenewTimerSet()
        {
        }

        /// <summary>
        /// Holds a set of <see cref="MessageLockRenewTimer"/> instances for every provided <see cref="BrokeredMessage"/>.
        /// Returns an instance from this[get].
        /// </summary>
        /// <param name="messages"></param>
        /// <param name="messageLockRenewTimeSpan"></param>
        /// <param name="logAction">Logger callback.</param>
        public MessageLockRenewTimerSet(ICollection<BrokeredMessage> messages, TimeSpan messageLockRenewTimeSpan, Action<string> logAction)
        {
            _timers = new Dictionary<BrokeredMessage, MessageLockRenewTimer>(messages.Count);
            foreach (var message in messages)
            {
                _timers.Add(message, new MessageLockRenewTimer(message, messageLockRenewTimeSpan, logAction));
            }
        }

        /// <summary>
        /// Returns the timer for the provided message, or a dummy if no timer exists.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public IDisposable this[BrokeredMessage message]
        {
            get
            {
                MessageLockRenewTimer timer;
                if (_timers != null && _timers.TryGetValue(message, out timer))
                {
                    return timer;
                }
                return Dummy;
            }
        }

        private static readonly IDisposable Dummy = new DummyDisposable();

        private class DummyDisposable : IDisposable
        {
            public void Dispose()
            {
            }
        }

        public void Dispose()
        {
            if (_timers == null) return;

            foreach (var timer in _timers.Values.ToArray())
            {
                timer.Dispose();
            }
        }
    }
}