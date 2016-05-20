using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Streamstone.StreamCollection
{

    public sealed partial class StreamCollectionDictionary
    {
        public const string StreamCollectionPropertyName = "StreamCollectionDictionary";

        /// <summary>
        /// Provisions new stream in the specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>The stream header</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If stream already exists in the partition
        /// </exception>
        public static StreamCollectionDictionary Provision(Partition partition, int partitionSize = 0)
        {
            return Provision(new StreamCollectionDictionary(partition, partitionSize));
        }

        /// <summary>
        /// Provisions new stream  with the given properties in the specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <param name="properties">The stream properties</param>
        /// <returns>The stream header</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        /// If stream already exists in the partition
        /// </exception>
        public static StreamCollectionDictionary Provision(Partition partition, StreamCollectionDictionaryProperties properties, int partitionSize = 0)
        {
            return Provision(new StreamCollectionDictionary(partition, properties, partitionSize));
        }

        /// <summary>
        /// Provisions specified stream.
        /// </summary>
        /// <param name="stream">The transient stream header.</param>
        /// <returns>The updated, persistent stream header</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="stream"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If stream already exists in the partition
        /// </exception>
        static StreamCollectionDictionary Provision(StreamCollectionDictionary StreamCollectionDictionary)
        {
            Requires.NotNull(StreamCollectionDictionary, "StreamCollectionDictionary");
            return new ProvisionOperation(StreamCollectionDictionary).Execute();
        }

        /// <summary>
        /// Initiates an asynchronous operation that provisions new stream in the specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>The promise, that wil eventually return stream header or will fail with exception</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If stream already exists in the partition
        /// </exception>
        public static Task<StreamCollectionDictionary> ProvisionAsync(Partition partition, int partitionSize = 0)
        {
            return ProvisionAsync(new StreamCollectionDictionary(partition, partitionSize));
        }

        /// <summary>
        /// Initiates an asynchronous operation that provisions new stream with the given properties in the specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <param name="properties">The stream properties</param>
        /// <returns>The promise, that wil eventually return stream header or will fail with exception</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        /// If stream already exists in the partition
        /// </exception>        
        public static Task<StreamCollectionDictionary> ProvisionAsync(Partition partition, StreamCollectionDictionaryProperties properties, int partitionSize = 0)
        {
            return ProvisionAsync(new StreamCollectionDictionary(partition, properties, partitionSize));
        }

        /// <summary>
        /// Initiates an asynchronous operation that provisions specified stream.
        /// </summary>
        /// <param name="stream">The transient stream header.</param>
        /// <returns>The promise, that wil eventually return updated, persistent stream header or will fail with exception</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="stream"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If stream already exists in the partition
        /// </exception>
        static Task<StreamCollectionDictionary> ProvisionAsync(StreamCollectionDictionary StreamCollectionDictionary)
        {
            Requires.NotNull(StreamCollectionDictionary, "StreamCollectionDictionary");
            return new ProvisionOperation(StreamCollectionDictionary).ExecuteAsync();
        }

        /// <summary>
        /// Sets the given stream properties (metadata).
        /// </summary>
        /// <param name="stream">The stream header.</param>
        /// <param name="properties">The properties.</param>
        /// <returns>Updated stream header</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="stream"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     If given stream header represents a transient stream
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If stream has been changed in storage after the given stream header has been read
        /// </exception>
        public static StreamCollectionDictionary SetProperties(StreamCollectionDictionary stream, StreamCollectionDictionaryProperties properties)
        {
            Requires.NotNull(stream, "stream");
            Requires.NotNull(properties, "properties");

            if (stream.IsTransient)
                throw new ArgumentException("Can't set properties on transient stream", "stream");

            return new SetPropertiesOperation(stream, properties).Execute();
        }

         /// <summary>
        /// Initiates an asynchronous operation that sets the given stream properties (metadata).
        /// </summary>
        /// <param name="stream">The stream header.</param>
        /// <param name="properties">The properties.</param>
        /// <returns>The promise, that wil eventually return updated stream header or will fail with exception</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="stream"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     If given stream header represents a transient stream
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If stream has been changed in storage after the given stream header has been read
        /// </exception>
        public static Task<StreamCollectionDictionary> SetPropertiesAsync(StreamCollectionDictionary stream, StreamCollectionDictionaryProperties properties)
        {
            Requires.NotNull(stream, "stream");
            Requires.NotNull(properties, "properties");

            if (stream.IsTransient)
                throw new ArgumentException("Can't set properties on transient stream", "stream");

            return new SetPropertiesOperation(stream, properties).ExecuteAsync();
        }

        /// <summary>
        /// Opens the stream in specified partition. Basically, it just return a stream header.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>The stream header</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="StreamNotFoundException">
        ///     If there is no stream in a given partition
        /// </exception>
        public static StreamCollectionDictionary Open(Partition partition)
        {
            var result = TryOpen(partition);

            if (result.Found)
                return result.Stream;

            throw new StreamNotFoundException(partition);
        }

        /// <summary>
        /// Initiates an asynchronous operation that opens the stream in specified partition. Basically, it just return a stream header.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>
        ///     The promise, that wil eventually return the stream header or wil fail with exception
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="StreamNotFoundException">
        ///     If there is no stream in a given partition
        /// </exception>
        public static async Task<StreamCollectionDictionary> OpenAsync(Partition partition)
        {
            var result = await TryOpenAsync(partition).Really();

            if (result.Found)
                return result.Stream;

            throw new StreamNotFoundException(partition);
        }

        /// <summary>
        /// Tries to open the stream in a specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>
        ///     The result of stream open operation, which could be further examined for stream existence
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        public static StreamCollectionDictionaryOpenResult TryOpen(Partition partition)
        {
            Requires.NotNull(partition, "partition");

            return new OpenOperation(partition).Execute();
        }

        /// <summary>
        /// Initiates an asynchronous operation that tries to open the stream in a specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>
        ///     The promise, that wil eventually return the result of stream open operation, 
        ///     which could be further examined for stream existence;  or wil fail with exception
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        public static Task<StreamCollectionDictionaryOpenResult> TryOpenAsync(Partition partition)
        {
            Requires.NotNull(partition, "partition");

            return new OpenOperation(partition).ExecuteAsync();
        }

        /// <summary>
        /// Checks if there is a stream exists in the specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>
        ///     <c>true</c> if stream header was found in the specified partition, <c>false</c> otherwise
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        public static bool Exists(Partition partition)
        {
            return TryOpen(partition).Found;
        }

        /// <summary>
        /// Initiates an asynchronous operation that checks if there is a stream exists in the specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>
        ///     The promise, that wil eventually return <c>true</c>
        ///     if stream header was found in the specified partition,  <c>false</c> otherwise
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        public static async Task<bool> ExistsAsync(Partition partition)
        {
            return (await TryOpenAsync(partition).Really()).Found;
        }

        /// <summary>
        /// Sets the given stream properties (metadata).
        /// </summary>
        /// <param name="stream">The stream header.</param>
        /// <param name="properties">The properties.</param>
        /// <returns>Updated stream header</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="stream"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     If given stream header represents a transient stream
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If stream has been changed in storage after the given stream header has been read
        /// </exception>
        public static Stream ConnectStream(StreamCollectionDictionary streamCollection, Stream stream)
        {
            Requires.NotNull(stream, "stream");
            Requires.NotNull(streamCollection, "streamCollection");

            if (streamCollection.IsTransient)
                throw new ArgumentException("Transient streamCollection can't track streams ", "streamCollection");

            if (!streamCollection.Partition.Table.Uri.Equals(stream.Partition.Table.Uri))
            {
                throw new ArgumentException("streamCollection can't track stream from different table", "streamCollection");
            }


            var properties = stream.Properties.ToDictionary(x => x.Key, x => x.Value);

            var streamCollectionKeys = GetStreamCollectionKeys(properties);

            streamCollectionKeys[streamCollection.Partition.Key]= streamCollection.PropertyHeader();

            SetStreamCollectionKeys(properties, streamCollectionKeys);

            return Stream.SetProperties(stream, StreamProperties.From(properties));
        }

        

        private static void SetStreamCollectionKeys(Dictionary<string, EntityProperty> properties, Dictionary<string, StreamCollectionDictionaryPropertyHeader> streamCollectionKeys)
        {

            properties[StreamCollectionDictionary.StreamCollectionPropertyName] = new EntityProperty(JsonConvert.SerializeObject(streamCollectionKeys));

        }

        private static Dictionary<string, StreamCollectionDictionaryPropertyHeader> GetStreamCollectionKeys( Dictionary<string, EntityProperty> properties)
        {

            Dictionary<string, StreamCollectionDictionaryPropertyHeader> streamCollectionKeys = new Dictionary<string, StreamCollectionDictionaryPropertyHeader>();
            if (properties.ContainsKey(StreamCollectionPropertyName))
            {
                streamCollectionKeys = JsonConvert.DeserializeObject<Dictionary<string, StreamCollectionDictionaryPropertyHeader>>(properties[StreamCollectionDictionary.StreamCollectionPropertyName].StringValue);
            }
            return streamCollectionKeys;
        }

        /// <summary>
        /// Initiates an asynchronous operation that sets the given stream properties (metadata).
        /// </summary>
        /// <param name="stream">The stream header.</param>
        /// <param name="properties">The properties.</param>
        /// <returns>The promise, that wil eventually return updated stream header or will fail with exception</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="stream"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     If given stream header represents a transient stream
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If stream has been changed in storage after the given stream header has been read
        /// </exception>
        public static Task<StreamCollectionDictionary> ConnectStreamAsync(StreamCollectionDictionary stream, StreamCollectionDictionaryProperties properties)
        {
            Requires.NotNull(stream, "stream");
            Requires.NotNull(properties, "properties");

            if (stream.IsTransient)
                throw new ArgumentException("Can't set properties on transient stream", "stream");

            return new SetPropertiesOperation(stream, properties).ExecuteAsync();
        }


        public static void TrackStream(Stream current, Stream original)
        {
            Requires.NotNull(current, "current");

            if (current.IsTransient)
                throw new ArgumentException("Transient streamCollection can't track streams ", "streamCollection");
            Requires.NotNull(original, "original");

            if (original.IsTransient)
                throw new ArgumentException("Transient streamCollection can't track streams ", "streamCollection");



            var properties = current.Properties.ToDictionary(x => x.Key, x => x.Value);

            var streamCollectionKeys = GetStreamCollectionKeys(properties);
            foreach (var streamCollectionPropertyHeader in streamCollectionKeys.Values)
            {
                var streamCollection = StreamCollectionDictionary.From(current, streamCollectionPropertyHeader);
                streamCollection.RecordStream(current, original);
            }

        }

        private void RecordStream(Stream current, Stream original)
        {
                try
                {
                    Console.WriteLine("GlobalDictionary true");
                    //var retrievedEntity = current;

                    
                    
                    string postFix = "";
                    string dictionaryPartition = this.Partition.Key;
                    int partitionKeySize = this.PartitionSize;

                    if (!string.IsNullOrEmpty(current.Partition.RowKeyPrefix))
                    {
                        postFix = current.Partition.RowKeyPrefix;
                    }
                    else
                    {
                        postFix = current.Partition.PartitionKey;
                    }
                    postFix = postFix.Substring(0, partitionKeySize);

                    var properties = current.Properties.ToDictionary(x => x.Key, x => x.Value);

                    
                    


                    var entity = new StreamCollectionStreamEntity(dictionaryPartition + "-" + postFix, current.Partition.Key, "", original.Version, current.Version, current.Partition.Key);
                    //entity.RowKey = entity.PartitionKey + "-gd-" + entity.RowKey;


                    // Create a retrieve operation that takes a customer entity.
                    TableOperation retrieveOperation = 
                    TableOperation.Retrieve<StreamCollectionStreamEntity>(entity.PartitionKey, entity.RowKey);
                    
                    // Execute the operation.
                    TableResult retrievedResult = Partition.Table.Execute(retrieveOperation);

                    // Assign the result to a CustomerEntity object.
                    StreamCollectionStreamEntity updateEntity = (StreamCollectionStreamEntity)retrievedResult.Result;

                    if (updateEntity != null)
                    {
                        if (entity.CurrentStreamVersion > updateEntity.CurrentStreamVersion || entity.OriginalStreamVersion < updateEntity.OriginalStreamVersion)
                        {
                            updateEntity.CurrentStreamVersion = Math.Min(entity.OriginalStreamVersion, updateEntity.OriginalStreamVersion);
                            updateEntity.CurrentStreamVersion = Math.Max(entity.CurrentStreamVersion, updateEntity.CurrentStreamVersion);

                            // Create the InsertOrReplace TableOperation.
                            TableOperation replaceOperation = TableOperation.Replace(updateEntity);

                            // Execute the operation.
                            Partition.Table.Execute(replaceOperation);

                            //Console.WriteLine("Entity was updated.");

                        }
                    }

                    else
                    {
                        TableOperation to = TableOperation.Insert(entity);
                        Partition.Table.Execute(to);
                    }
                }
                catch (StorageException e)
                {
                    //batch.Handle(table, e);
                }

        }




        /// <summary>
        /// Reads the events from a stream in a specified partition.
        /// </summary>
        /// <typeparam name="T">The type of event entity to return</typeparam>
        /// <param name="partition">The partition.</param>
        /// <param name="startVersion">The start version.</param>
        /// <param name="sliceSize">Size of the slice.</param>
        /// <returns>
        ///     The slice of the stream, which contains events that has been read
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     If <paramref name="startVersion"/> &lt; 1
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     If <paramref name="sliceSize"/> &lt; 1
        /// </exception>       
        /// <exception cref="StreamNotFoundException">
        ///     If there is no stream in a given partition
        /// </exception>
        public static List<StreamCollectionStreamEntity> Read(StreamCollectionDictionary streamCollection)
        {            
            return new ReadOperation(streamCollection).Execute();
        }

        /// <summary>
        /// Initiates an asynchronous operation that reads the events from a stream in a specified partition.
        /// </summary>
        /// <typeparam name="T">The type of event entity to return</typeparam>
        /// <param name="partition">The partition.</param>
        /// <param name="startVersion">The start version.</param>
        /// <param name="sliceSize">Size of the slice.</param>
        /// <returns>
        ///     The promise, that wil eventually return the slice of the stream, 
        ///     which contains events that has been read; or will fail with exception
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     If <paramref name="startVersion"/> &lt; 1
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     If <paramref name="sliceSize"/> &lt; 1
        /// </exception>       
        /// <exception cref="StreamNotFoundException">
        ///     If there is no stream in a given partition
        /// </exception>
        public static Task<List<StreamCollectionStreamEntity>> ReadAsync(StreamCollectionDictionary streamCollection) 
        {

            return new ReadOperation(streamCollection).ExecuteAsync();
        }

    }
}