using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.StreamCollection
{
    class StreamCollectionLogEntity : TableEntity
    {
        public const string FixedRowKey = "STREAM-COLLECTION-LOG-HEAD";

        public StreamCollectionLogEntity()
        {
            Properties = StreamCollectionLogProperties.None;
        }

        public StreamCollectionLogEntity(Partition partition, string etag,long timespanWindowSize, bool consolidateStreams, StreamCollectionLogProperties properties)
        {
            Partition = partition;
            PartitionKey = partition.PartitionKey;
            RowKey = partition.StreamCollectionLogRowKey();
            ETag = etag;
            Properties = properties;
            TimespanWindowSize = timespanWindowSize;
            ConsolidateStreams = consolidateStreams;
        }

        public long TimespanWindowSize { get; set; }
        public bool ConsolidateStreams { get; set; }
         
        public StreamCollectionLogProperties Properties  { get; set; }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);
            Properties = StreamCollectionLogProperties.ReadEntity(properties);
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var result = base.WriteEntity(operationContext);
            Properties.WriteTo(result);
            return result;
        }

        public static StreamCollectionLogEntity From(DynamicTableEntity entity)
        {
            return new StreamCollectionLogEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                Properties = StreamCollectionLogProperties.From(entity)                
            };
        }

        public EntityOperation Operation()
        {
            var isTransient = ETag == null;

            return isTransient
                    ? (EntityOperation) 
                      new EntityOperation.Insert(this)
                    : new EntityOperation.Replace(this);
        }

        [IgnoreProperty]
        public Partition Partition
        {
            get; set;
        }
    }
}