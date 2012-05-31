using System;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SnowMaker.Data.MongoDB
{
    public class MongoOptimisticDataStore : IOptimisticDataStore
    {
        public MongoServerSettings ServerSettings { get; set; }

        private MongoServer Server { get; set; }

        private MongoCollection Collection { get; set; }

        public string DatabaseName { get; set; }

        public MongoDatabase Database { get; set; }

        public string CollectionName { get; set; }

        public bool IsConnected { get; set; }

        public MongoOptimisticDataStore(string serverAddress, string databaseName, string collectionName)
        {
            this.ServerSettings = new MongoServerSettings();
            this.ServerSettings.Server = new MongoServerAddress(serverAddress);
            this.DatabaseName = databaseName;
            this.CollectionName = collectionName;

            this.IsConnected = false;
            
        }

        public MongoOptimisticDataStore(MongoServerSettings settings, string databaseName, string collectionName)
        {
            this.ServerSettings = settings;
            this.DatabaseName = databaseName;
            this.CollectionName = collectionName;

            this.IsConnected = false;
        }

        public MongoOptimisticDataStore()
        {
            this.ServerSettings = new MongoServerSettings();
            this.ServerSettings.ConnectionMode = ConnectionMode.Direct;
            this.ServerSettings.Server = new MongoServerAddress("localhost");

            this.DatabaseName = "SnowMaker";
            this.CollectionName = "Identities";

            Connect();
        }

        public void Connect()
        {
            this.Server = new MongoServer(this.ServerSettings);
            this.Database = this.Server.GetDatabase(this.DatabaseName);

            if (!this.Database.CollectionExists(this.CollectionName))
            {
                this.Database.CreateCollection(this.CollectionName);
            }

            this.Collection = this.Database.GetCollection<SnowflakeBlock>(this.CollectionName);
        }


        public string GetData(string blockName)
        {
            if (string.IsNullOrWhiteSpace(blockName))
            {
                throw new ArgumentNullException("blockName", "blockName must not be null or empty string");
            }

            if (!this.IsConnected)
            {
                this.Connect();
            }

            if (this.Collection == null)
            {
                throw new InvalidOperationException("Collection must not be null");
            }

            SnowflakeBlock block = this.Collection.FindOneByIdAs<SnowflakeBlock>(new BsonString(blockName));
            if (block == null)
            {
                return "1";
            }

            return block.Data;
        }

        public bool TryOptimisticWrite(string blockName, string data)
        {
            if (string.IsNullOrWhiteSpace(blockName))
            {
                throw new ArgumentNullException("blockName", "blockName must not be null or empty string");
            }

            if (this.Collection == null)
            {
                throw new InvalidOperationException("Collection must not be null");
            }

            SnowflakeBlock block = this.Collection.FindOneByIdAs<SnowflakeBlock>(new BsonString(blockName));

            if (block == null)
            {
                block = new SnowflakeBlock() { Name = blockName, Data = data };

            }
            else
            {
                block.Data = data;
            }

            MongoInsertOptions insertOptions = new MongoInsertOptions(this.Collection);
            insertOptions.SafeMode = new SafeMode(true);

            SafeModeResult result = this.Collection.Save<SnowflakeBlock>(block, insertOptions);
            if (result == null)
            {
                return false;
            }

            return result.Ok;
        }



        
    }
}
