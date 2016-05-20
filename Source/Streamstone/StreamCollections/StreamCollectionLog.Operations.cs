using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.StreamCollection
{
    public sealed partial class StreamCollectionLog
    {
        class ProvisionOperation
        {
            readonly StreamCollectionLog StreamCollectionLog;
            readonly CloudTable table;

            public ProvisionOperation(StreamCollectionLog StreamCollectionLog)
            {
                Debug.Assert(StreamCollectionLog.IsTransient);
                
                this.StreamCollectionLog = StreamCollectionLog;
                table = StreamCollectionLog.Partition.Table;
            }

            public StreamCollectionLog Execute()
            {
                var insert = new Insert(StreamCollectionLog);

                try
                {
                    table.Execute(insert.Prepare());
                }
                catch (StorageException e)
                {
                    insert.Handle(table, e);
                }

                return insert.Result();
            }

            public async Task<StreamCollectionLog> ExecuteAsync()
            {
                var insert = new Insert(StreamCollectionLog);

                try
                {
                    await table.ExecuteAsync(insert.Prepare()).Really();
                }
                catch (StorageException e)
                {
                    insert.Handle(table, e);
                }

                return insert.Result();
            }

            class Insert
            {
                readonly StreamCollectionLogEntity StreamCollectionLog;
                readonly Partition partition;

                public Insert(StreamCollectionLog StreamCollectionLog)
                {
                    this.StreamCollectionLog = StreamCollectionLog.Entity();
                    partition = StreamCollectionLog.Partition;
                }

                public TableOperation Prepare()
                {
                    return TableOperation.Insert(StreamCollectionLog);
                }

                internal void Handle(CloudTable table, StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    throw exception.PreserveStackTrace();
                }

                internal StreamCollectionLog Result()
                {
                    return From(partition, StreamCollectionLog);
                }
            }
        }

        class WriteOperation
        {
            const int MaxOperationsPerChunk = 99;

            readonly StreamCollectionLog StreamCollectionLog;
            readonly CloudTable table;
            readonly IEnumerable<RecordedEvent> events;

            public WriteOperation(StreamCollectionLog StreamCollectionLog, IEnumerable<EventData> events)
            {
                this.StreamCollectionLog = StreamCollectionLog;
                //this.events = StreamCollectionLog.Record(events);
                table = StreamCollectionLog.Partition.Table;
            }

            public StreamCollectionLogWriteResult Execute()
            {
                var current = StreamCollectionLog;

                foreach (var chunk in Chunks())
                {
                    var batch = chunk.ToBatch(current);

                    try
                    {
                        table.ExecuteBatch(batch.Prepare());
                    }
                    catch (StorageException e)
                    {
                        batch.Handle(table, e);
                    }

                    current = batch.Result();
                }

                //GlobalDictionary(current);
                //GlobalLog(current);
                return new StreamCollectionLogWriteResult(current, events.ToArray());
            }

//            private void GlobalDictionary(StreamCollectionLog current)
//          {
//              if (true||current.Properties.Any(x => x.Key == "GlobalDictionary" && (x.Value.BooleanValue ?? true)))
//              {
//                  try
//                  {
//                      Console.WriteLine("GlobalDictionary true");
//                      var retrievedEntity = current.Entity();
//
//
//
//                      string postFix = "";
//                      string dictionaryPartition = current.Properties.Any(x => x.Key == "GlobalDictionaryPartitionKeyBase")
//                          ? current.Properties.Where(x => x.Key == "GlobalDictionaryPartitionKeyBase").FirstOrDefault().Value.StringValue
//                          : "GlobalDictionary";
//                      int partitionKeySize = current.Properties.Any(x => x.Key == "GlobalDictionaryPartitionKeySize")
//                          ? current.Properties.Where(x => x.Key == "GlobalDictionaryPartitionKeySize").FirstOrDefault().Value.Int32Value.Value
//                          : 2;
//
//                      if (!string.IsNullOrEmpty(current.Partition.RowKeyPrefix))
//                      {
//                          //{ "GlobalDictionaryPartitionKeySize", new EntityProperty(0)},
//                          //{ "GlobalDictionaryPartitionKeyBase", new EntityProperty("GlobalDictionaryPartition")},
//                          //{ "GlobalDictionary",  new EntityProperty(true)}
//
//                          //partition key is the stream id
//                          postFix = current.Partition.RowKeyPrefix;
//
//                      }
//                      else
//                      {
//                          postFix = current.Partition.PartitionKey;
//                      }
//                      postFix = postFix.Substring(0, partitionKeySize);
//
//                      var entity = current.Entity();
//
//                      entity.RowKey = entity.PartitionKey + "-gd-" + entity.RowKey;
//                      entity.PartitionKey = dictionaryPartition + "-" + postFix;
//
//                      // Create a retrieve operation that takes a customer entity.
//                      TableOperation 
//                      retrieveOperation = TableOperation.Retrieve<StreamCollectionLogEntity>(entity.PartitionKey, entity.RowKey);
//
//                      // Execute the operation.
//                      TableResult 
//                      retrievedResult = table.Execute(retrieveOperation);
//
//                      // Assign the result to a CustomerEntity object.
//                      StreamCollectionLogEntity updateEntity = (StreamCollectionLogEntity)retrievedResult.Result;
//
//                      if (updateEntity != null)
//                      {
//                          if (entity.Version > updateEntity.Version)
//                          {
//                              updateEntity.Version = entity.Version;
//
//                              // Create the InsertOrReplace TableOperation.
//                              TableOperation replaceOperation = TableOperation.Replace(updateEntity);
//
//                              // Execute the operation.
//                              table.Execute(replaceOperation);
//
//                              Console.WriteLine("Entity was updated.");
//
//                          }
//                      }
//
//                      else
//                      {
//                          TableOperation to = TableOperation.Insert(entity);
//                          table.Execute(to);
//                      }
//                  }
//                  catch (StorageException e)
//                  {
//                      //batch.Handle(table, e);
//                  }
//
//              }
//          }
//
//            public static DateTime Floor(DateTime date, TimeSpan span)
//          {
//              long ticks = (date.Ticks / span.Ticks);
//              return new DateTime(ticks * span.Ticks);
//          }
//            private void GlobalLog(StreamCollectionLog current)
//          {
//              if (true || current.Properties.Any(x => x.Key == "GlobalLog" && (x.Value.BooleanValue ?? true)))
//              {
//                  try
//                  {
//                      Console.WriteLine("GlobalLog true");
//                      var retrievedEntity = current.Entity();
//
//                      // Create a retrieve operation that takes a customer entity.
//                      TableOperation retrieveOperation = TableOperation.Retrieve<StreamCollectionLogEntity>(retrievedEntity.PartitionKey, retrievedEntity.RowKey);
//
//                      // Execute the operation.
//                      TableResult retrievedResult = table.Execute(retrieveOperation);
//
//                      // Assign the result to a CustomerEntity object.
//                      retrievedEntity = (StreamCollectionLogEntity)retrievedResult.Result;
//                      if (retrievedEntity != null && retrievedEntity.Version == 0/*current.Version*/)
//                      {
//
//
//                          string logPartition = current.Properties.Any(x => x.Key == "GlobalLogPartitionKeyBase")
//                              ? current.Properties.Where(x => x.Key == "GlobalLogPartitionKeyBase").FirstOrDefault().Value.StringValue
//                              : "GlobalLog";
//                          TimeSpan logTimeWindow = current.Properties.Any(x => x.Key == "GlobalLogTimeWindow")
//                              ? TimeSpan.FromTicks(current.Properties.Where(x => x.Key == "GlobalLogTimeWindow").FirstOrDefault().Value.Int64Value.Value)
//                              : TimeSpan.FromSeconds(4);
//
//                          DateTime dateFull = retrievedEntity.Timestamp.UtcDateTime;
//                          DateTime dateRounded = Floor(dateFull, logTimeWindow);
//
//                          retrievedEntity.RowKey = string.Format("{0:D19}", dateFull.Ticks) + "-gd-" + retrievedEntity.PartitionKey + "-gd-" + retrievedEntity.RowKey + "-gd-" + string.Format("{0:d10}", retrievedEntity.Version);
//                          retrievedEntity.PartitionKey = logPartition + "-" + string.Format("{0:D19}", dateRounded.Ticks);
//
//
//                              TableOperation to = TableOperation.Insert(retrievedEntity);
//                              table.Execute(to);
//                          
//                      }
//                  }
//                  catch (StorageException e)
//                  {
//                      //batch.Handle(table, e);
//                  }
//
//              }
//          }

            public async Task<StreamCollectionLogWriteResult> ExecuteAsync()
            {
                var current = StreamCollectionLog;

                foreach (var chunk in Chunks())
                {
                    var batch = chunk.ToBatch(current);

                    try
                    {
                        await table.ExecuteBatchAsync(batch.Prepare()).Really();
                    }
                    catch (StorageException e)
                    {
                        batch.Handle(table, e);
                    }

                    current = batch.Result();
                }

                return new StreamCollectionLogWriteResult(current, events.ToArray());
            }

            IEnumerable<Chunk> Chunks()
            {
                return Chunk.Split(events).Where(s => !s.IsEmpty);
            }

            class Chunk
            {
                public static IEnumerable<Chunk> Split(IEnumerable<RecordedEvent> events)
                {
                    var current = new Chunk();

                    foreach (var @event in events)
                    {
                        var next = current.Add(@event);

                        if (next != current)
                            yield return current;

                        current = next;
                    }

                    yield return current;
                }

                readonly List<RecordedEvent> events = new List<RecordedEvent>();
                int operations;

                Chunk()
                {}

                Chunk(RecordedEvent first)
                {
                    Accomodate(first);
                }

                Chunk Add(RecordedEvent @event)
                {
                    if (@event.Operations > MaxOperationsPerChunk)
                        throw new InvalidOperationException(
                            string.Format("{0} include(s) in event {1}:{{{2}}}, plus event entity itself, is over Azure's max batch size limit [{3}]",
                                          @event.IncludedOperations.Length, @event.Version, @event.Id, MaxOperationsPerChunk));
                    
                    if (!CanAccomodate(@event))
                        return new Chunk(@event);

                    Accomodate(@event);
                    return this;
                }

                void Accomodate(RecordedEvent @event)
                {
                    operations += @event.Operations;
                    events.Add(@event);
                }

                bool CanAccomodate(RecordedEvent @event)
                {
                    return operations + @event.Operations <= MaxOperationsPerChunk;
                }

                public bool IsEmpty
                {
                    get { return events.Count == 0; }
                }

                public Batch ToBatch(StreamCollectionLog stream)
                {
                    var entity = stream.Entity();
                    return new Batch(entity, events);
                }
            }

            class Batch
            {
                readonly List<EntityOperation> operations = 
                     new List<EntityOperation>();
                
                readonly StreamCollectionLogEntity stream;
                readonly List<RecordedEvent> events;
                readonly Partition partition;

                internal Batch(StreamCollectionLogEntity stream, List<RecordedEvent> events)
                {
                    this.stream = stream;
                    this.events = events;
                    partition = stream.Partition;
                }

                internal TableBatchOperation Prepare()
                {
                    WriteStream();
                    WriteEvents();
                    WriteIncludes();

                    return ToBatch();
                }

                void WriteStream()
                {
                    operations.Add(stream.Operation());
                }

                void WriteEvents()
                {
                    operations.AddRange(events.SelectMany(e => e.EventOperations));
                }

                void WriteIncludes()
                {
                    var tracker = new EntityChangeTracker();

                    foreach (var @event in events)
                        tracker.Record(@event.IncludedOperations);

                    operations.AddRange(tracker.Compute());
                }

                TableBatchOperation ToBatch()
                {
                    var result = new TableBatchOperation();
                    
                    foreach (var each in operations)
                        result.Add(each);

                    return result;
                }

                internal StreamCollectionLog Result()
                {
                    return From(partition, stream);
                }

                internal void Handle(CloudTable table, StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    if (exception.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                        throw exception.PreserveStackTrace();

                    var error = exception.RequestInformation.ExtendedErrorInformation;
                    if (error.ErrorCode != "EntityAlreadyExists")
                        throw UnexpectedStorageResponseException.ErrorCodeShouldBeEntityAlreadyExists(error);

                    var position = ParseConflictingEntityPosition(error);

                    Debug.Assert(position >= 0 && position < operations.Count);
                    var conflicting = operations[position].Entity;

                    if (conflicting == stream)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    var id = conflicting as EventIdEntity;
                    if (id != null)
                        throw new DuplicateEventException(partition, id.Event.Id);

                    var @event = conflicting as EventEntity;
                    if (@event != null)
                        throw ConcurrencyConflictException.EventVersionExists(partition, @event.Version);

                    var include = operations.Single(x => x.Entity == conflicting); 
                    throw IncludedOperationConflictException.Create(partition, include);
                }

                static int ParseConflictingEntityPosition(StorageExtendedErrorInformation error)
                {
                    var lines = error.ErrorMessage.Split('\n');
                    if (lines.Length != 3)
                        throw UnexpectedStorageResponseException.ConflictExceptionMessageShouldHaveExactlyThreeLines(error);

                    var semicolonIndex = lines[0].IndexOf(":", StringComparison.Ordinal);
                    if (semicolonIndex == -1)
                        throw UnexpectedStorageResponseException.ConflictExceptionMessageShouldHaveSemicolonOnFirstLine(error);

                    int position;
                    if (!int.TryParse(lines[0].Substring(0, semicolonIndex), out position))
                        throw UnexpectedStorageResponseException.UnableToParseTextBeforeSemicolonToInteger(error);

                    return position;
                }
            }
        }

        class SetPropertiesOperation
        {
            readonly StreamCollectionLog stream;
            readonly CloudTable table;
            readonly StreamCollectionLogProperties properties;

            public SetPropertiesOperation(StreamCollectionLog stream, StreamCollectionLogProperties properties)
            {                
                this.stream = stream;
                this.properties = properties;
                table = stream.Partition.Table;
            }

            public StreamCollectionLog Execute()
            {
                var replace = new Replace(stream, properties);

                try
                {
                    table.Execute(replace.Prepare());
                }
                catch (StorageException e)
                {
                    replace.Handle(table, e);
                }

                return replace.Result();
            }

            public async Task<StreamCollectionLog> ExecuteAsync()
            {
                var replace = new Replace(stream, properties);

                try
                {
                    await table.ExecuteAsync(replace.Prepare()).Really();
                }
                catch (StorageException e)
                {
                    replace.Handle(table, e);
                }

                return replace.Result();
            }

            class Replace
            {
                readonly StreamCollectionLogEntity stream;
                readonly Partition partition;

                public Replace(StreamCollectionLog stream, StreamCollectionLogProperties properties)
                {
                    this.stream = stream.Entity();
                    this.stream.Properties = properties;
                    partition = stream.Partition;
                }

                internal TableOperation Prepare()
                {                    
                    return TableOperation.Replace(stream);
                }

                internal void Handle(CloudTable table, StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                        throw ConcurrencyConflictException.StreamChanged(partition);

                    throw exception.PreserveStackTrace();
                }

                internal StreamCollectionLog Result()
                {
                    return From(partition, stream);
                }
            }
        }

        class OpenOperation
        {
            readonly Partition partition;
            readonly CloudTable table;

            public OpenOperation(Partition partition)
            {
                this.partition = partition;
                table = partition.Table;
            }

            public StreamCollectionLogOpenResult Execute()
            {
                return Result(table.Execute(Prepare()));
            }

            public async Task<StreamCollectionLogOpenResult> ExecuteAsync()
            {
                return Result(await table.ExecuteAsync(Prepare()));
            }

            TableOperation Prepare()
            {
                return TableOperation.Retrieve<StreamCollectionLogEntity>(partition.PartitionKey, partition.StreamCollectionLogRowKey());
            }

            StreamCollectionLogOpenResult Result(TableResult result)
            {
                var entity = result.Result;

                return entity != null
                           ? new StreamCollectionLogOpenResult(true, From(partition, (StreamCollectionLogEntity)entity))
                           : StreamCollectionLogOpenResult.NotFound;
            }
        }

        class ReadOperation
        {
            readonly StreamCollectionLog StreamCollection;
            readonly DateTime? StartTime;
            readonly DateTime? EndTime;

            public ReadOperation(StreamCollectionLog streamCollection)
            {
                StreamCollection = streamCollection;
                StartTime = null;
                EndTime = null;
            }
            public ReadOperation(StreamCollectionLog streamCollection, DateTime startTime, DateTime endTime)
            {
                StreamCollection = streamCollection;

                StartTime = StreamCollectionLog.Floor(startTime,streamCollection.TimespanWindowSize);
                EndTime = StreamCollectionLog.Floor(endTime, streamCollection.TimespanWindowSize).AddTicks(streamCollection.TimespanWindowSize.Ticks - 1);
                Console.WriteLine(string.Format("{0:D19} - {1:D19}", StartTime.Value.Ticks, EndTime.Value.Ticks));
                if (StartTime >= EndTime)
                {
                    throw new ArgumentException("startTime must be less then endtime", "startTime");
                }

            }

            public List<StreamCollectionStreamEntity> Execute()
            {
                return Result(ExecuteQuery(PrepareQuery()));
            }

            public async Task<List<StreamCollectionStreamEntity>> ExecuteAsync()
            {
                return Result(await ExecuteQueryAsync(PrepareQuery()));
            }

            List<StreamCollectionStreamEntity> Result(ICollection<DynamicTableEntity> entities)
            {
                var list = new List<StreamCollectionStreamEntity>();
                var table = StreamCollection.Partition.Table;

                return entities.Select(x => StreamCollectionStreamEntity.From(x)).ToList();
            }

            TableQuery<DynamicTableEntity> PrepareQuery()
            {


                var partitionKeyStart = StreamCollection.Partition.PartitionKey;
                if (StartTime.HasValue)
                {
                    partitionKeyStart += "-" + string.Format("{0:D19}", StartTime.Value.Ticks);
                }
                else
                {
                    partitionKeyStart += "-";
                }
                var partitionKeyEnd = StreamCollection.Partition.PartitionKey;
                if (EndTime.HasValue)
                {
                    partitionKeyEnd += "-" + string.Format("{0:D19}", EndTime.Value.Ticks);
                }
                else
                {
                    partitionKeyEnd += "~";
                }
                // ReSharper disable StringCompareToIsCultureSpecific

                var query = StreamCollection.Partition.Table
                    .CreateQuery<DynamicTableEntity>()
                    .Where(x => x.PartitionKey.CompareTo(partitionKeyStart)  >= 0
                                   && x.PartitionKey.CompareTo(partitionKeyEnd) <= 0);

                return (TableQuery<DynamicTableEntity>) query;
            }

            List<DynamicTableEntity> ExecuteQuery(TableQuery<DynamicTableEntity> query)
            {
                var result = new List<DynamicTableEntity>();
                TableContinuationToken token = null;

                do
                {
                    var segment = StreamCollection.Partition.Table.ExecuteQuerySegmented(query, token);
                    token = segment.ContinuationToken;
                    result.AddRange(segment.Results);
                }
                while (token != null);

                return result;
            }

            async Task<List<DynamicTableEntity>> ExecuteQueryAsync(TableQuery<DynamicTableEntity> query)
            {
                var result = new List<DynamicTableEntity>();
                TableContinuationToken token = null;

                do
                {
                    var segment = await StreamCollection.Partition.Table.ExecuteQuerySegmentedAsync(query, token).Really();
                    token = segment.ContinuationToken;
                    result.AddRange(segment.Results);
                }
                while (token != null);

                return result;
            }

        }
    }
}
