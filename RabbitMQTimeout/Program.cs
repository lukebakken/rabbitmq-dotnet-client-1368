// See https://aka.ms/new-console-template for more information
using RabbitMQTimeout;

Console.WriteLine("Hello, World!");

bool _initializedConnection = false;

var _rabbitMQCore = new RabbitMQCore();
_rabbitMQCore.OnConnectionShutdown += _rabbitMQCore_OnConnectionShutdown;

void _rabbitMQCore_OnConnectionShutdown()
{
    Console.WriteLine("Close external handler executed");
    _initializedConnection = false;
}

while (true)
{
    try
    {
        if (!_initializedConnection)
        {
            _rabbitMQCore.InitializeConnection();
            _initializedConnection = true;
        }
    }
    catch (Exception)
    {
        _initializedConnection = false;
    }

    if (_initializedConnection)
    {
        try
        {
            for (int i = 0; i < 10; i++)
            {
                _rabbitMQCore.CreateChannel($"MyChannel{i}");
            }

            Console.WriteLine("START CLUMSY NOW");
            Thread.Sleep(5000);

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                if (!_initializedConnection)
                {
                    break;
                }
                _rabbitMQCore.CreateQueue($"MyChannel{i}", $"MyQueue{i}", "msg");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error {ex.GetType()}. {ex.Message}");
        }
    }
}
