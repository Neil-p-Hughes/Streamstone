using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamstone.StreamCollection
{
    /// <summary>
    /// Represents an event stream. Instances of this class enapsulate stream header information such as version, etag,  metadata, etc;
    /// while static methods are used to manipulate stream.
    /// </summary>
    public sealed partial class StreamCollectionDictionary
    {


        /// <summary>
        /// The additional properties (metadata) of this stream
        /// </summary>
        public readonly StreamCollectionDictionaryProperties Properties;

        /// <summary>
        /// The partition in which this stream resides.
        /// </summary>
        public readonly Partition Partition;
        
        /// <summary>
        /// The latest etag
        /// </summary>
        public readonly string ETag;

        /// <summary>
        /// The version of the stream. Sequential, monotonically increasing, no gaps.
        /// </summary>
        public readonly int PartitionSize;

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance which doesn't have any additional properties.
        /// </summary>
        /// <param name="partition">
        /// The partition in which this stream will reside. 
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        public StreamCollectionDictionary(Partition partition, int partitionSize = 0) 
            : this(partition, StreamCollectionDictionaryProperties.None, partitionSize)
        {}

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance with the given additional properties.
        /// </summary>
        /// <param name="partition">
        /// The partition in which this stream will reside. 
        /// </param>
        /// <param name="properties">
        /// The additional properties for this stream.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        public StreamCollectionDictionary(Partition partition, StreamCollectionDictionaryProperties properties, int partitionSize = 0)
        {
            Requires.NotNull(partition, "partition");
            Requires.NotNull(properties, "properties");

            Partition = partition;
            Properties = properties;
        }

        internal StreamCollectionDictionary(Partition partition, string etag, int partitionSize, StreamCollectionDictionaryProperties properties)
        {
            Partition = partition;
            ETag = etag;
            PartitionSize = partitionSize;
            Properties = properties;
        }

        /// <summary>
        /// Gets a value indicating whether this stream header represents a transient stream.
        /// </summary>
        /// <value>
        /// <c>true</c> if this stream header was newed; otherwise, <c>false</c>.
        /// </value>
        public bool IsTransient
        {
            get { return ETag == null; }
        }

        /// <summary>
        /// Gets a value indicating whether this stream header represents a persistent stream.
        /// </summary>
        /// <value>
        /// <c>true</c> if this stream header has been obtained from storage; otherwise, <c>false</c>.
        /// </value>
        public bool IsPersistent
        {
            get { return !IsTransient; }
        }

        static StreamCollectionDictionary From(Partition partition, StreamCollectionDictionaryEntity entity)
        {
            return new StreamCollectionDictionary(partition, entity.ETag, entity.PartitionSize, entity.Properties);
        }

        StreamCollectionDictionaryEntity Entity()
        {
            return new StreamCollectionDictionaryEntity(Partition, ETag, PartitionSize,  Properties);
        }

        static StreamCollectionDictionary From(CloudTable table, StreamCollectionDictionaryPropertyHeader PropertyHeader)
        {
            Partition partition = new Partition(table, PropertyHeader.PartitionKey);
            return new StreamCollectionDictionary(partition, PropertyHeader.PartitionSize);
        }
        static StreamCollectionDictionary From(Stream trackedStream, StreamCollectionDictionaryPropertyHeader PropertyHeader)
        {
            Partition partition = new Partition(trackedStream.Partition.Table, PropertyHeader.PartitionKey);
            return new StreamCollectionDictionary(partition, PropertyHeader.PartitionSize);
        }

        StreamCollectionDictionaryPropertyHeader PropertyHeader()
        {
            return StreamCollectionDictionaryPropertyHeader.From(this);
        }
    }
}
