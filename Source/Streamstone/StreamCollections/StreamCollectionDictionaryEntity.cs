using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.StreamCollection
{
    class StreamCollectionDictionaryEntity : TableEntity
    {
        public const string FixedRowKey = "STREAM-COLLECTION-DICTIONARY-HEAD";

        public StreamCollectionDictionaryEntity()
        {
            Properties = StreamCollectionDictionaryProperties.None;
        }

        public StreamCollectionDictionaryEntity(Partition partition, string etag,int partitionSize, StreamCollectionDictionaryProperties properties)
        {
            Partition = partition;
            PartitionKey = partition.PartitionKey;
            RowKey = partition.StreamCollectionDictionaryRowKey();
            ETag = etag;
            Properties = properties;
            PartitionSize = partitionSize;
        }

        public int PartitionSize { get; set; }
        public StreamCollectionDictionaryProperties Properties  { get; set; }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);
            Properties = StreamCollectionDictionaryProperties.ReadEntity(properties);
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var result = base.WriteEntity(operationContext);
            Properties.WriteTo(result);
            return result;
        }

        public static StreamCollectionDictionaryEntity From(DynamicTableEntity entity)
        {
            return new StreamCollectionDictionaryEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                Properties = StreamCollectionDictionaryProperties.From(entity)
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