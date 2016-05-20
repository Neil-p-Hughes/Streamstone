using System;
using System.Linq;
using Streamstone;
using Streamstone.StreamCollection;
using System.Diagnostics;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace Example.Scenarios
{
    public class SC06_Read_from_log : Scenario
    {
        public override void Run()
        {
            Prepare();
            ReadStreamCollection();


            WriteReadAsParallel();

            ReadStreamCollection();

        }



        void ReadStreamCollection()
        {

            StreamCollectionLog streamCollectionLog = BuildStreamCollectionLog();

            var streams = StreamCollectionLog.Read(streamCollectionLog, DateTime.UtcNow.AddSeconds(-60), DateTime.UtcNow.AddSeconds(-40));

            Console.WriteLine("");
            foreach (var item in streams)
            {
                
                Console.WriteLine("Stream {0} in log has changed version from {1} to {2}", item.StreamKey, item.OriginalStreamVersion, item.CurrentStreamVersion);
            }
            
        }

        void WriteReadAsParallel()
        {
            const int streamsToWrite = 3;

            StreamCollectionLog streamCollectionLog = BuildStreamCollectionLog();

            List<bool> WritingFinished = new List<bool>();

            foreach (var item in Enumerable.Range(1, streamsToWrite))
            {
                WritingFinished.Add(false);
            }

            Enumerable.Range(1, streamsToWrite).AsParallel()
                .ForAll(streamIndex =>
                {
                    if (streamIndex == 1)
                    {
                        WritingFinished[streamIndex - 1] = true;
                        while (WritingFinished.Count(b => b == true) < WritingFinished.Count)
                        {
                            ReadStreamCollection();
                        }
                    }
                    else
                    {

                        var partition = new Partition(Partition.Table, $"{Id}-{streamIndex}");

                        var existent = Stream.TryOpen(partition);

                        var stream = existent.Found
                            ? existent.Stream
                            : Stream.Provision(partition);

                        stream = StreamCollectionLog.ConnectStream(streamCollectionLog, stream);


                        var stopwatch = Stopwatch.StartNew();

                        for (int i = 1; i <= 30; i++)
                        {
                            var events = Enumerable.Range(1, 10)
                                .Select(_ => Event(new InventoryItemCheckedIn(partition.Key, i * 1000 + streamIndex)))
                                .ToArray();

                            var result = Stream.Write(stream, events);

                            stream = result.Stream;
                        }

                        stopwatch.Stop();
                        WritingFinished[streamIndex - 1] = true;
                        Console.WriteLine("Finished writing 300 events to new stream in partition '{0}' in {1}ms", stream.Partition, stopwatch.ElapsedMilliseconds);
                    }
                });

        }

        private StreamCollectionLog BuildStreamCollectionLog()
        {
            var streamCollectionLogPartition = new Partition(Partition.Table, "StreamCollectionLog");
            var existentStreamCollectionLog = StreamCollectionLog.TryOpen(streamCollectionLogPartition);
            var streamCollectionLog = existentStreamCollectionLog.Found
                ? existentStreamCollectionLog.Stream
                : StreamCollectionLog.Provision(streamCollectionLogPartition, TimeSpan.FromSeconds(30), true);
            return streamCollectionLog;
        }

        static EventData Event(int id)
        {
            var properties = new
            {
                Id = id,
                Type = "<type>",
                Data = "{some}"
            };

            return new EventData(EventId.From(id.ToString()), EventProperties.From(properties));
        }

        class EventEntity
        {
            public int Id      { get; set; }
            public string Type { get; set; }
            public string Data { get; set; }
            public int Version { get; set; }
        }

        void Prepare()
        {
            const int streamsToWrite = 5;

            StreamCollectionLog streamCollectionLog = BuildStreamCollectionLog();


            Enumerable.Range(1, streamsToWrite).AsParallel()
                .ForAll(streamIndex =>
                {
                    var partition = new Partition(Partition.Table, $"{Id}-{streamIndex}");

                    var existent = Stream.TryOpen(partition);

                    var stream = existent.Found
                        ? existent.Stream
                        : Stream.Provision(partition);

                    stream = StreamCollectionLog.ConnectStream(streamCollectionLog, stream);


                    Console.WriteLine("Writing to new stream in partition '{0}'", partition);
                    var stopwatch = Stopwatch.StartNew();

                    for (int i = 1; i <= 30; i++)
                    {
                        var events = Enumerable.Range(1, 10)
                            .Select(_ => Event(new InventoryItemCheckedIn(partition.Key, i * 1000 + streamIndex)))
                            .ToArray();

                        var result = Stream.Write(stream, events);

                        stream = result.Stream;
                    }

                    stopwatch.Stop();
                    Console.WriteLine("Finished writing 300 events to new stream in partition '{0}' in {1}ms", stream.Partition, stopwatch.ElapsedMilliseconds);
                });
        }

        static EventData Event(object e)
        {
            var id = Guid.NewGuid();

            var properties = new
            {
                Id = id,                 // id that you specify for Event ctor is used only for duplicate event detection
                Type = e.GetType().Name, // you can include any number of custom properties along with event
                Data = JSON(e),          // you're free to choose any name you like for data property
                Bin = BSON(e)            // and any storage format: binary, string, whatever (any EdmType)
            };

            return new EventData(EventId.From(id), EventProperties.From(properties));
        }

        static string JSON(object data)
        {
            return JsonConvert.SerializeObject(data);
        }

        static byte[] BSON(object data)
        {
            var stream = new System.IO.MemoryStream();

            using (var writer = new BsonWriter(stream))
            {
                var serializer = new JsonSerializer();
                serializer.Serialize(writer, data);
            }

            return stream.ToArray();
        }
    }
}
