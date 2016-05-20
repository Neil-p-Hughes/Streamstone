using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.StreamCollection
{
    /// <summary>
    /// Represents the collection of named stream properties (metadata)
    /// </summary>
    public sealed class StreamCollectionDictionaryProperties : PropertyMap
    {
        /// <summary>
        /// An empty collection of stream properties
        /// </summary>
        public static readonly StreamCollectionDictionaryProperties None = new StreamCollectionDictionaryProperties();

        StreamCollectionDictionaryProperties()
        {}

        StreamCollectionDictionaryProperties(IDictionary<string, EntityProperty> properties) 
            : base(properties)
        {}

        internal static StreamCollectionDictionaryProperties ReadEntity(IDictionary<string, EntityProperty> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(properties);
        }

        /// <summary>
        /// Creates new instance of <see cref="StreamProperties"/> class using given dictionary of entity properties
        /// </summary>
        /// <param name="properties">The properties.</param>
        /// <returns>New instance of <see cref="StreamProperties"/> class</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        public static StreamCollectionDictionaryProperties From(IDictionary<string, EntityProperty> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(Clone(properties));
        }

        /// <summary>
        /// Creates new instance of <see cref="StreamProperties"/> class using public properties of a given object.
        /// All public properties should be of WATS compatible type..
        /// </summary>
        /// <param name="obj">The properties.</param>
        /// <returns>New instance of <see cref="StreamProperties"/> class</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="obj"/> is <c>null</c>
        /// </exception>
        /// <exception cref="NotSupportedException">
        ///     If <paramref name="obj"/> has properties of WATS incompatible type
        /// </exception>
        public static StreamCollectionDictionaryProperties From(object obj)
        {
            Requires.NotNull(obj, "obj");
            return Build(ToDictionary(obj));
        }

        /// <summary>
        /// Creates new instance of <see cref="StreamProperties"/> class using public properties of a given table entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>New instance of <see cref="StreamProperties"/> class</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="entity"/> is <c>null</c>
        /// </exception>
        public static StreamCollectionDictionaryProperties From(ITableEntity entity)
        {
            Requires.NotNull(entity, "entity");
            return Build(ToDictionary(entity));
        }

        static StreamCollectionDictionaryProperties Build(IEnumerable<KeyValuePair<string, EntityProperty>> properties)
        {
            var filtered = properties
                .Where(x => !IsReserved(x.Key))
                .ToDictionary(p => p.Key, p => p.Value);

            return new StreamCollectionDictionaryProperties(filtered);
        }

        static bool IsReserved(string propertyName)
        {
            switch (propertyName)
            {
                case "PartitionKey":
                case "RowKey":
                case "ETag":
                case "Timestamp":
                case "partitionSize":
                    return true;
                default:
                    return false;
            }
        }
    }
}