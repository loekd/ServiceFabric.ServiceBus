using System;
using System.Threading;
using Microsoft.ServiceBus.Messaging;

namespace ServiceFabric.ServiceBus.Services.CommunicationListeners
{
    /// <summary>
    /// Automatically renews the lock on the provided message.
    /// </summary>
    internal sealed class MessageLockRenewTimer : IDisposable
    {
        private readonly BrokeredMessage _message;
        private readonly Action<string> _logAction;
        private readonly Timer _timer;
        private readonly string _messageId;

        /// <summary>
        /// Gets the interval at which message locks are renewed.
        /// </summary>
        public TimeSpan MessageLockRenewTimeSpan { get; }

        /// <summary>
        /// Starts a timer that renews the lock on the provided message, until this instance is disposed.
        /// </summary> 
        public MessageLockRenewTimer(BrokeredMessage message, TimeSpan messageLockRenewTimeSpan, Action<string> logAction)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            _message = message;
            _logAction = logAction;
            _messageId = message.MessageId;
            MessageLockRenewTimeSpan = messageLockRenewTimeSpan;
            //renew after lock duration expires, and repeat
            _timer = new Timer(Tick, 1, MessageLockRenewTimeSpan, MessageLockRenewTimeSpan);
        }

        /// <summary>
        /// Renew the lock on the message.
        /// </summary>
        /// <param name="state"></param>
        private void Tick(object state)
        {
            const int maxDepth = 10;
            int depth = (int)state;

            try
            {
                if (depth > maxDepth)
                {
                    throw new Exception("Retry maximum exceeded.");
                }
                _message.RenewLock();
                WriteLog($"Renewed lock for BrokeredMessage {_messageId}.");
            }
            catch (MessagingCommunicationException)
            {
                Tick(depth + 1);
            }
            catch (MessagingException ex)
            {
                if (ex.IsTransient)
                {
                    Thread.Sleep(500);
                    Tick(depth + 1);
                }
                else
                {
                    WriteLog($"Failed to renew lock for BrokeredMessage {_messageId}. Error:'{ex.Message}'");
                    Dispose();
                }
            }
            catch (ObjectDisposedException)
            {
                Dispose();
            }
            catch (Exception ex)
            {
                WriteLog($"Failed to renew lock for BrokeredMessage {_messageId}. Error:'{ex.Message}'");
                Dispose();
            }
        }

        private void WriteLog(string message)
        {
            _logAction?.Invoke(message);
        }

        /// <summary>
        /// Stop the timer.
        /// </summary>
        public void Dispose()
        {
            _timer.Dispose();
        }
    }
}