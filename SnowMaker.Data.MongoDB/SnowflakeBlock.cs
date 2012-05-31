using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson.Serialization.Attributes;

namespace SnowMaker.Data.MongoDB
{
    public class SnowflakeBlock
    {
        [BsonId]
        public string Name { get; set; }

        public string Data { get; set; }
    }
}
