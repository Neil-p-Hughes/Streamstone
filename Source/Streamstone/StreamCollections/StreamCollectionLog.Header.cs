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
    public sealed partial class StreamCollectionLog
    {


        /// <summary>
        /// The additional properties (metadata) of this stream
        /// </summary>
        public readonly StreamCollectionLogProperties Properties;

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
        public readonly TimeSpan TimespanWindowSize;
        public readonly bool ConsolidateStreams;

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance which doesn't have any additional properties.
        /// </summary>
        /// <param name="partition">
        /// The partition in which this stream will reside. 
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        public StreamCollectionLog(Partition partition, TimeSpan timespanWindowSize, bool consolidateStreams) 
            : this(partition, StreamCollectionLogProperties.None, timespanWindowSize, consolidateStreams)
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
        public StreamCollectionLog(Partition partition, StreamCollectionLogProperties properties, TimeSpan timespanWindowSize, bool consolidateStreams)
        {
            Requires.NotNull(partition, "partition");
            Requires.NotNull(properties, "properties");

            Partition = partition;
            Properties = properties;
            TimespanWindowSize = timespanWindowSize;
            ConsolidateStreams = consolidateStreams;
        }

        internal StreamCollectionLog(Partition partition, string etag, TimeSpan timespanWindowSize, StreamCollectionLogProperties properties, bool consolidateStreams)
        {
            Partition = partition;
            ETag = etag;
            TimespanWindowSize = timespanWindowSize;
            Properties = properties;
            ConsolidateStreams = consolidateStreams;
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

        static StreamCollectionLog From(Partition partition, StreamCollectionLogEntity entity)
        {
            return new StreamCollectionLog(partition, entity.ETag, TimeSpan.FromTicks(entity.TimespanWindowSize), entity.Properties, entity.ConsolidateStreams);
        }

        StreamCollectionLogEntity Entity()
        {
            return new StreamCollectionLogEntity(Partition, ETag, TimespanWindowSize.Ticks,ConsolidateStreams,  Properties);
        }

        static StreamCollectionLog From(CloudTable table, StreamCollectionLogPropertyHeader PropertyHeader)
        {
            Partition partition = new Partition(table, PropertyHeader.PartitionKey);
            return new StreamCollectionLog(partition, PropertyHeader.TimespanWindowSize, PropertyHeader.ConsolidateStreams);
        }
        static StreamCollectionLog From(Stream trackedStream, StreamCollectionLogPropertyHeader PropertyHeader)
        {
            Partition partition = new Partition(trackedStream.Partition.Table, PropertyHeader.PartitionKey);
            return new StreamCollectionLog(partition, PropertyHeader.TimespanWindowSize,PropertyHeader.ConsolidateStreams);
        }

        StreamCollectionLogPropertyHeader PropertyHeader()
        {
            return StreamCollectionLogPropertyHeader.From(this);
        }
    }
}
