using RabbitMQ.Client;
using System.Collections.Concurrent;
using RabbitMQClient = RabbitMQ.Client;

namespace RabbitMQTimeout
{
    internal class RabbitMQCore
    {
        #region Private properties

        private IConnection _connection;

        /// <summary>
        /// Key: Channel name. Value: Channel
        /// </summary>
        private readonly Dictionary<string, IModel> _channels = new Dictionary<string, IModel>();

        /// <summary>
        /// Key: Channel name. Value: Queue name
        /// </summary>
        private readonly Dictionary<string, string> _queues = new Dictionary<string, string>();

        /// <summary>
        /// Key: Channel name. Value: Lock object for channel operations
        /// </summary>
        private readonly ConcurrentDictionary<string, object> _channelLock = new ConcurrentDictionary<string, object>();

        /// <summary>
        /// Key: Channel number. Value: Channel name
        /// </summary>
        private readonly Dictionary<int, string> _channelNumbers = new Dictionary<int, string>();

        private ConnectionFactory _connectionFactory = new ConnectionFactory()
        {
            HostName = "localhost",
            Port = 5672,
            UserName = "guest",
            ContinuationTimeout = TimeSpan.FromSeconds(5),
            Password = "guest",
            AutomaticRecoveryEnabled = false,
            TopologyRecoveryEnabled = false,
            ClientProvidedName = "MyClient"
        };

        /// <summary>
        /// Fired when the connection is shutdown.
        /// </summary>
        public event Action OnConnectionShutdown;

        #endregion Private properties

        #region public methods

        /// <summary>
        /// Initializes the RabbitMQ connection.
        /// </summary>
        /// <param name="config">The RabbitMQ configuration.</param>
        /// <exception cref="EtlException">Throw when occur an error initializing the connection.</exception>
        public void InitializeConnection()
        {
            try
            {
                CreateConnection();
            }
            catch (Exception ex)
            {
                Dispose();
                string error = $"Error connecting to host {ex.Message}";
                Console.WriteLine(error);
                throw;
            }
        }

        /// <summary>
        /// Create a channel with a given name.
        /// </summary>
        /// <param name="channel">Channel to create</param>
        public bool CreateChannel(string channel)
        {
            ValidateString(channel, nameof(channel));

            CheckIsInitializedConnection();
            if (_channels.ContainsKey(channel))
                return false;

            Console.WriteLine($"Opening channel {channel} in connection '{_connection.ClientProvidedName}'");
            IModel createChannel = _connection.CreateModel();
            Console.WriteLine($"Channel {channel} opened in connection '{_connection.ClientProvidedName}'");
            createChannel.ModelShutdown += (sender, e) => ChannelShutdown(sender, e, channel);
            createChannel.ConfirmSelect();
            createChannel.ExchangeDeclare(exchange: "myExchange", type: ExchangeType.Topic, durable: true);
            AddChannel(channel, createChannel);
            return true;
        }

        /// <summary>
        /// Create a queue within channel, with a name and link with routing key and channel max concurrency level.
        /// </summary>
        /// <param name="channel">Channel to create new queue.</param>
        /// <param name="queue">Name of the queue to create.</param>
        /// <param name="routingKey">Routing key name.</param>
        /// <exception cref="EtlException">Throw when occur an error while creating queue in channel.</exception>
        public bool CreateQueue(string channel, string queue, string routingKey)
        {
            CheckIsInitializedConnection();
            CheckIsInitializedChannel(channel);
            ValidateString(queue, nameof(queue));
            ValidateString(routingKey, nameof(routingKey));

            bool changeOrCreate = false;

            if (!_queues.ContainsKey(channel))
            {
                IModel chan = _channels[channel];

                var arguments = new Dictionary<string, object>
                {
                    { "x-queue-type", "quorum" },
                };

                Console.WriteLine($"Creating queue {queue}");
                bool retrying = true;
                while (retrying)
                {
                    try
                    {
                        chan.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false, arguments: arguments);
                        chan.QueueBind(queue, "myExchange", routingKey);
                        chan.BasicQos(0, 2, false);
                        retrying = false;
                    }
                    catch
                    {
                        try
                        {
                            CloseChannel(channel);
                        }
                        catch
                        {
                            // Ignore all exceptions here
                        }
                        finally
                        {
                            CreateChannel(channel);
                            retrying = true;
                            Console.WriteLine($"RETRY: Creating queue {queue}");
                        }
                    }
                }

                _queues.Add(channel, queue);
                changeOrCreate = true;

                Console.WriteLine($"Queue {queue} created");
            }

            return changeOrCreate;
        }

        /// <summary>
        /// Closes all channels, freeing up associated resources.
        /// </summary>
        public void Dispose()
        {
            var list = _channels.Keys.ToList();
            foreach (var channel in list)
            {
                CloseChannel(channel);
            }
            ClearChannels();
            CloseConnection();
        }

        #endregion public methods

        #region helper methods

        /// <summary>
        /// Checks if the connection is initialized.
        /// </summary>
        /// <exception cref="EtlException">Throw when the connection isn't initialized.</exception>
        private void CheckIsInitializedConnection()
        {
            if (_connection == null)
            {
                string error = $"{typeof(RabbitMQCore).Name} is not initialized. You must call InitializeConnection method";
                throw new Exception(error);
            }
        }

        /// <summary>
        /// Checks if the channel is initialized.
        /// </summary>
        /// <exception cref="EtlException">Throw when the channel isn't initialized.</exception>
        private void CheckIsInitializedChannel(string channel)
        {
            if (!_channels.ContainsKey(channel))
            {
                string error = $"{channel} is not initialized. You must call CreateChannel method";
                throw new Exception(error);
            }
        }

        /// <summary>
        /// Create a RabbitMQ connection.
        /// </summary>
        private void CreateConnection()
        {
            if (_connection != null)
                return;

            Console.WriteLine($"Opening connection '{_connectionFactory.ClientProvidedName}'");

            _connection = _connectionFactory.CreateConnection();
            _connection.ConnectionShutdown += _connection_ConnectionShutdown;

            Console.WriteLine($"Opened connection '{_connectionFactory.ClientProvidedName}'");
        }

        private void _connection_ConnectionShutdown(object? sender, ShutdownEventArgs e)
        {
            Console.WriteLine($"Connection to RabbitMQ shutdown. {e?.ReplyCode} - {e?.ReplyText}");

            CloseConnection();
        }

        /// <summary>
        /// Close a RabbitMQ connection, releasing all its resources.
        /// </summary>
        private void CloseConnection()
        {
            if (_connection == null)
                return;

            Console.WriteLine($"Closing connection '{_connection.ClientProvidedName}'");
            int? numHanlders = OnConnectionShutdown?.GetInvocationList().Count();
            if (numHanlders.HasValue)
                Console.WriteLine($"Close handlers: {numHanlders}");

            try
            {
                if (_connection.IsOpen)
                {
                    _connection.Close();
                    // Uncomment this to reset connection correctly
                    //_connection.Dispose();
                }

                // Comment this line to reset connection correctly
                _connection.Dispose();

                _connection = null;

                Console.WriteLine("Executing external handler");
                OnConnectionShutdown?.Invoke();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error closing conection {ex.GetType()}. {ex.Message}");
            }
        }

        /// <summary>
        /// Close a RabbitMQ channel, releasing all its resources. If the channel is closed due to an error, pending messages are monitored so that they are not lost.
        /// </summary>
        /// <param name="channelName">Name of channel to close.</param>
        private void CloseChannel(string channelName)
        {
            if (!_channels.ContainsKey(channelName))
                return;

            var channel = _channels[channelName];
            Console.WriteLine($"Closing channel '{channelName}({channel?.ChannelNumber})' of connection '{_connection?.ClientProvidedName}'");

            var chanLock = _channelLock.GetOrAdd(channelName, new object());
            lock (chanLock)
            {
                if (channel?.IsOpen ?? false)
                {
                    channel?.Close();
                    channel?.Dispose();
                }

                RemoveChannel(channelName);
            }
        }

        /// <summary>
        /// Add channel to all dictionaries that use it. Initialized the sent messages dictionary to store that messages.
        /// </summary>
        /// <param name="channel">Name of channel to add.</param>
        /// <param name="iModel">IModel object of the channel to add.</param>
        private void AddChannel(string channel, IModel iModel)
        {
            _channels.Add(channel, iModel);
            _channelNumbers.Add(iModel.ChannelNumber, channel);
        }

        /// <summary>
        /// Clear all channel dictionaries.
        /// </summary>
        private void ClearChannels()
        {
            _channels.Clear();
            _channelNumbers.Clear();
        }

        /// <summary>
        /// Removes the channel from all dictionaries and unsubscribe all events by the channel.
        /// </summary>
        /// <param name="channel">Name of channel.</param>
        private void RemoveChannel(string channel)
        {
            if (!_channels.ContainsKey(channel))
                return;

            _channelNumbers.Remove(_channels[channel].ChannelNumber);

            _channels[channel].ModelShutdown -= (sender, e) => ChannelShutdown(sender, e, channel);

            _queues.Remove(channel);
            _channels.Remove(channel);
        }

        private void ValidateString(string value, string parameter)
        {
            if (string.IsNullOrEmpty(value))
                throw new ArgumentNullException(parameter);
        }


        #endregion helper methods

        #region event handlers

        /// <summary>
        /// Handler to manage an event when a channel shutdown.
        /// </summary>
        /// <param name="sender">Objet of type IModel, that represent the channel shutdown.</param>
        /// <param name="e">Contains all the information about the event.</param>
        /// <param name="channel">Name of channel shutdown.</param>
        private void ChannelShutdown(object sender, ShutdownEventArgs e, string channel)
        {
            if (!_channels.ContainsKey(channel))
                return;

            Console.WriteLine($"Channel '{channel}({_channels[channel]?.ChannelNumber})' shutdown: {e?.ReplyCode}. {e?.ReplyText}");

            // If shutdown gracefully do nothing
            if (e.ReplyCode == RabbitMQClient.Constants.ReplySuccess)
            {
                CloseChannel(channel);
                return;
            }

            try
            {
                CloseChannel(channel);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error closing channel in connection {_connection?.ClientProvidedName}.", ex);
            }
        }

        #endregion event handlers
    }
}
