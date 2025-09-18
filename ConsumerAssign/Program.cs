using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Confluent.Kafka;

namespace ConsumerAssign
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test",
                EnableAutoCommit = false, // optional: control commits manually
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            var myTopicPart0 = new TopicPartition("my-topic", new Partition(0));
            var myOtherTopicPart2 = new TopicPartition("my-other-topic", new Partition(2));

            // Explicitly assign partitions
            var partitions = new List<TopicPartition> { };

            partitions.Add(myTopicPart0);
            partitions.Add(myOtherTopicPart2);

            consumer.Assign(partitions);

            Console.WriteLine("Waiting for messages from assigned partitions...");

            try
            {
                while (true)
                {
                    var cr = consumer.Consume(10);
                    //Console.WriteLine(
                    //    $"Consumed message '{cr?.Value}' at: {cr?.TopicPartitionOffset} in a topic '{cr?.Topic}'"
                    //);
                    if (cr is not null)
                    {
                        Console.WriteLine(
                            $"topic: '{cr?.Topic}' partition: '{cr?.Partition}' offset: '{cr?.Offset}' key: '{cr?.Message.Key}' value: '{cr?.Message.Value}'"
                        );
                    }
                }
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Consume error: {e.Error.Reason}");
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
