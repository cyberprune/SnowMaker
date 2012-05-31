using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit;
using NUnit.Framework;
using MongoDB.Driver;


namespace SnowMaker.Data.MongoDB.IntegrationTests
{
    [TestFixture]
    public class MongoDbTests
    {
        [Test]
        public void NewBlockShouldBe1()
        {
            MongoOptimisticDataStore ds = new MongoOptimisticDataStore();

            Assert.NotNull(ds);

            Guid blockName = Guid.NewGuid();

            Assert.AreEqual("1", ds.GetData(blockName.ToString()));

        }

        [Test]
        public void NewBlockShouldWrite()
        {
            MongoOptimisticDataStore ds = new MongoOptimisticDataStore();

            Assert.NotNull(ds);

            Guid blockName = Guid.NewGuid();

            bool result = ds.TryOptimisticWrite(blockName.ToString(), "0");

            Assert.True(result);


        }

        [Test]
        public void ShouldWriteSubsequentBlocks()
        {
            MongoOptimisticDataStore ds = new MongoOptimisticDataStore();

            Assert.NotNull(ds);

            Guid blockName = Guid.NewGuid();

            bool result = ds.TryOptimisticWrite(blockName.ToString(), "0");

            Assert.True(result);

            result = ds.TryOptimisticWrite(blockName.ToString(), "10");

            Assert.True(result);


        }


        [Test]
        public void ShouldReturnSameResult()
        {
            MongoOptimisticDataStore ds = new MongoOptimisticDataStore();

            Assert.NotNull(ds);

            string blockName = Guid.NewGuid().ToString();
            string data = Guid.NewGuid().ToString();

            bool result = ds.TryOptimisticWrite(blockName, data);

            Assert.True(result);

            string value = ds.GetData(blockName.ToString());

            Assert.AreEqual(data, value);
        }

        [Test]
        public void UniqueIdGeneratorWithMongoDBDataSource()
        {
            UniqueIdGenerator generator = new UniqueIdGenerator(new MongoOptimisticDataStore());
            string blockName = Guid.NewGuid().ToString();

            generator.BatchSize = 5;
            
            Assert.AreEqual(1, generator.NextId(blockName));
            Assert.AreEqual(2, generator.NextId(blockName));
            Assert.AreEqual(3, generator.NextId(blockName));
            Assert.AreEqual(4, generator.NextId(blockName));
            Assert.AreEqual(5, generator.NextId(blockName));
            Assert.AreEqual(6, generator.NextId(blockName));
            Assert.AreEqual(7, generator.NextId(blockName));
        }


        [Test]
        public void UniqueIdGeneratorWithMongoDBDataSourceMany()
        {
            UniqueIdGenerator gen1 = new UniqueIdGenerator(new MongoOptimisticDataStore());
            string blockName = Guid.NewGuid().ToString();

            gen1.BatchSize = 5;

            Assert.AreEqual(1, gen1.NextId(blockName));
            Assert.AreEqual(2, gen1.NextId(blockName));
            Assert.AreEqual(3, gen1.NextId(blockName));

            UniqueIdGenerator gen2 = new UniqueIdGenerator(new MongoOptimisticDataStore());
            gen2.BatchSize = 5;


            Assert.AreEqual(6, gen2.NextId(blockName));
            Assert.AreEqual(7, gen2.NextId(blockName));
            Assert.AreEqual(8, gen2.NextId(blockName));
            Assert.AreEqual(9, gen2.NextId(blockName));

            UniqueIdGenerator gen3 = new UniqueIdGenerator(new MongoOptimisticDataStore());
            gen3.BatchSize = 50;
            Assert.AreEqual(11, gen3.NextId(blockName));

            UniqueIdGenerator gen4 = new UniqueIdGenerator(new MongoOptimisticDataStore());
            gen4.BatchSize = 50;
            Assert.AreEqual(61, gen4.NextId(blockName));
          
        }

        [Test]
        public void NewBlockShouldBe1SpecificConstructor()
        {
            MongoServerSettings settings = new MongoServerSettings();
            settings.Server = new MongoServerAddress("localhost");

            MongoOptimisticDataStore ds = new MongoOptimisticDataStore(settings, "SnowMaker", "IntegrationTests");

            Assert.NotNull(ds);

            Guid blockName = Guid.NewGuid();

            Assert.AreEqual("1", ds.GetData(blockName.ToString()));

        }

        [Test]
        public void NewBlockShouldBe1StringConstructor()
        {
           

            MongoOptimisticDataStore ds = new MongoOptimisticDataStore("localhost", "SnowMaker", "IntegrationTests");

            Assert.NotNull(ds);

            Guid blockName = Guid.NewGuid();

            Assert.AreEqual("1", ds.GetData(blockName.ToString()));

        }
    }
}
